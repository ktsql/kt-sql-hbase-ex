package me.principality.ktsql.backend.hbase

import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.linq4j.AbstractEnumerable
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.Pair
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import java.nio.charset.Charset
import java.util.*

/**
 * 通过实现不同类型的表，优化查询性能
 * https://calcite.apache.org/docs/tutorial.html#optimizing-queries-using-planner-rules
 *
 * org.apache.calcite.schema 定义了几种常见的表
 *   org.apache.calcite.schema.ExtensibleTable
 *   org.apache.calcite.schema.FilterableTable
 *   org.apache.calcite.schema.ModifiableTable
 *   org.apache.calcite.schema.ProjectableFilterableTable
 *   org.apache.calcite.schema.QueryableTable
 *   org.apache.calcite.schema.ScannableTable
 *   org.apache.calcite.schema.StreamableTable
 *   org.apache.calcite.schema.TranslatableTable
 *
 * 关注：ScannableTable, FilterableTable, ProjectableFilterableTable, TranslatableTable
 *
 * adapter负责对接calcite，最终的实现通过hbase-client来完成，如果要实现计算下推，采用TranslatableTable
 *
 * 如果使用HBase作为后端存储，每个表的数据都可能存放在多个region上，
 * 要访问每个表的数据，则需要维系表对应的region list，并根据数据的获取需求，
 * 建立和多个region server的连接
 *
 * HBase加速查询的关键技术点：
 * 1. 通过多个region server实现数据并行读取，通过rowkey高效完成数据range判断
 * 2. 如果使用filter读取数据，可以在region server端完成数据过滤
 * 3. 通过压缩传输减少数据传输所需的时间（可以考虑结合压缩算法实现更高效的SQL查询）
 * 4. 部分aggregate处理可以下推到region server完成（需要与calcite配合实现）
 *
 * 读取数据的速度取决于region server和网络速度，配上万兆网卡或者RDMA等网络设备可有效提升速度
 *
 * SQL查询应尽量考虑把project, filter以及aggregate下推到region server，从而获得更好的性能
 */
abstract class HBaseTable : AbstractQueryableTable {
    protected val name: String
    protected val htableDescriptor: HTableDescriptor
    protected val isTransactional: Boolean // 默认为真，可以手动指明非真，用于快速插入数据
    protected val indexType: IndexType // 默认的索引方式，如果含索引，需要使用索引辅助类实现读写操作
    protected val secondaryIndexTable: KeyValueIndexTable?
    protected val columnDescriptors: List<ColumnType>

    companion object {
        val columnFamily: String = "cf"
        val primaryKey: String = "id"
    }

    constructor(name: String, descriptor: HTableDescriptor) : super(Array<Any>::class.java) {
        this.name = name
        this.htableDescriptor = descriptor
        this.columnDescriptors = getTableColumns(this.name)

        // 下面开始做一些变量的初始化：
        // 1. 是否为事务表
        isTransactional = getTableSystemAttribute(name, HBaseTable.SystemAttribute.IS_TRANSACTIONAL.name).toString().toBoolean()
        // 2. 索引的类型
        val typeAttr = getTableSystemAttribute(name, HBaseTable.SystemAttribute.INDEX_TYPE.name)
        val typeStr = String(typeAttr, Charset.forName("UTF-8"))
        indexType = HBaseTable.IndexType.valueOf(typeStr)
        if (indexType == HBaseTable.IndexType.KEY_VALUE) {
            // todo 完成索引表的初始化
            throw NotImplementedError("create index table helper")
        } else {
            secondaryIndexTable = null
        }
    }

    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?,
                                        schema: SchemaPlus?,
                                        tableName: String?): Queryable<T> {
        throw NotImplementedError("Need to be implemented")
    }

    /**
     * 通过该函数获得the names and types of a table's columns
     *
     * 可以通过hbase的表接口获得该信息，需要注意的是：
     * 如何实现从不同数据源数据类型映射/转换到Calcite数据类型的逻辑？只能通过元数据表
     *
     * HBase的column是不区分类型的，只能从系统表读取
     */
    override fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
        if (typeFactory == null) {
            throw IllegalArgumentException("RelDataTypeFactory is null")
        }

        val types = ArrayList<RelDataType>()
        val names = ArrayList<String>()

        // 遍历获得每一个column的类型
        for (column in columnDescriptors) {
            val sqlType = typeFactory.createSqlType(SqlTypeName.get(column.type))

            names.add(column.name)
            types.add(sqlType)
        }

        return typeFactory.createStructType(Pair.zip(names, types))
    }

    internal fun getHTable(name: String): Table {
        return HBaseConnection.connection().getTable(TableName.valueOf(name))
    }

    private fun getTableSystemAttribute(tableName: String, attribute: String): ByteArray {
        val get = Get(Bytes.toBytes(tableName))
        val systemhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))

        val dbresult = systemhtable.get(get)
        return dbresult.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(attribute))
    }

    /**
     * 这里的排序，需要根据创建表的次序来排
     */
    private fun getTableColumns(tableName: String): List<ColumnType> {
        val primaryAttribute = getTableSystemAttribute(tableName, HBaseTable.SystemAttribute.PRIMARY.name)
        val primaryKey = primaryAttribute.toString(Charset.forName("UTF-8"))

        val scan = Scan()
        scan.setRowPrefixFilter(Bytes.toBytes(name))
        val columnhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))
        val scanner = columnhtable.getScanner(scan)

        val array = ArrayList<ColumnType>()
        for (result in scanner) {
            val name = result.row
            val type = result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.DATA_TYPE.name))
            val precision = result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.PRECISION.name))
            val position = result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.POSITION.name))
            val isNullable = result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.NULLABLE.name))
            val defaultValue = result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.DEFAULT.name))

            val rowname = splitColumnRowkey(name.toString(Charset.forName("UTF-8")))
            val columnType = ColumnType(rowname,
                    type.toString(Charset.forName("UTF-8")),
                    precision.toString(Charset.forName("UTF-8")).toInt(),
                    position.toString(Charset.forName("UTF-8")).toInt(),
                    rowname.equals(primaryKey),
                    isNullable.toString().toBoolean(),
                    defaultValue.toString(Charset.forName("UTF-8")))

            array.add(columnType)
        }
        return array.sortedBy { it.position }
    }

    private fun splitColumnRowkey(s: String): String {
        return s.split(".").get(1) // 这里和schema的columnSystableRowkey一一对应
    }

    protected data class ColumnType(val name: String,
                                    val type: String,
                                    val precision: Int,
                                    val position: Int,
                                    val isPrimary: Boolean,
                                    val isNullable: Boolean,
                                    val defaultValue: String)

    /**
     * 支持的索引类型
     */
    enum class IndexType(s: String) {
        NONE("none"),
        KEY_VALUE("kv")
    }

    enum class LockStatus(s: String) {
        LOCKED("locked"),
        UNLOCK("unlock")
    }

    enum class PrimaryType(s: String) {
        UUID("uuid"),
        STRING("string")
    }

    /**
     * 对表的数据读取方式进行定义，
     * - 如果是全表扫描，过滤和投影由calcite完成
     * - 如果是过滤扫描，投影由calcite完成
     * - 如果是投影过滤，则calcite没有额外的工作
     *
     * 实现三种模式的处理，可以对性能进行比较测试
     */
    enum class Flavor {
        SCANNABLE, FILTERABLE, PROJECTFILTERABLE
    }

    enum class SystemAttribute(s: String) {
        TABLE_PATH("tablePath"),
        IS_TRANSACTIONAL("isTrans"),
        INDEX_TYPE("indexType"),
        LOCK_STATUS("lockStatus"),
        CREATE_TIME("createTime"),
        CHARSET("charset"),
        COMMENT("comment"),
        PRIMARY("primary"),
        INDEX("index")
    }

    enum class ColumnAttribute(s: String) {
        DEFAULT("default"),
        NULLABLE("nullable"),
        DATA_TYPE("datatype"),
        MAX_LENGTH("maxlen"),
        PRECISION("precsion"),
        POSITION("position"),
        COMMENT("comment")
    }

    class SqlEnumeratorImpl<T>(rs: Iterable<T>) : Enumerator<T> {
        private val results: List<T> = rs.toList()
        private var index: Int = 0

        override fun moveNext(): Boolean {
            if (index < results.size) {
                index++
                return true
            }
            return false
        }

        override fun current(): T {
            return results.get(index)
        }

        override fun reset() {
            index = 0
        }

        override fun close() {
            // 不需要
        }
    }

    class SqlEnumerableImpl<T>(rs: ResultScanner) : AbstractEnumerable<T>() {
        private val resultScanner: Iterable<T> = rs as Iterable<T>

        override fun enumerator(): Enumerator<T> {
            return SqlEnumeratorImpl(resultScanner)
        }
    }
}