package me.principality.ktsql.backend.hbase

import me.principality.ktsql.backend.hbase.HBaseUtils.SYSTEM_COLUMN_NAME
import me.principality.ktsql.backend.hbase.HBaseUtils.SYSTEM_TABLE_NAME
import me.principality.ktsql.backend.hbase.exception.IllegalColumnNameException
import me.principality.ktsql.backend.hbase.exception.IndexExistsException
import me.principality.ktsql.backend.hbase.exception.PrimaryKeyMissedException
import mu.KotlinLogging
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.type.RelDataTypeImpl
import org.apache.calcite.rel.type.RelProtoDataType
import org.apache.calcite.schema.ColumnStrategy
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql2rel.InitializerExpressionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes
import java.time.LocalDateTime


/**
 * Calcite对表的创建有几种：
 *
 * 1. 根据配置，初始化生成不同类型的表，
 * 2. 使用Schema对没有配置的Table进行创建
 *
 * 在HBase里面，没有database的概念，所有的表都在同一个级别上
 * 使用HBaseAdmin对应Schema
 *
 * HBaseSchema的核心函数是getTable，实例需维护 Map<String, Table>
 *
 * 根据HBase的架构，要读取数据时，首先需去RootRegion获取RowKey对应的MetaRegion，
 * 再从MetaRegion获得最终数据所在的RegionServer，此时再使用RegionClient读取数据
 *
 * 从jdbc调用后端的过程为：
 * 1. 创建connection，此时会完成schema的创建，schema需要准备好表创建的逻辑
 * 2. 通过connection获取statement，并通过statement执行sql，此时会创建表
 *
 * 在一个连接中，会出现多次读取NameMap的情况，如果在一个session里面反复rpc，
 * 对性能的消耗是很大的，所以要采用同一session的情况下获取信息用本地cache
 *
 * TODO calcite释放connection, admin, htable的机制，如何实现才是最优？
 *
 * 资源释放逻辑：
 * ResultSetEnumerator.close会调用statment和connection的close()
 * AvaticaConnection.prepareAndExecuteInternal会调用上个statment.resultset.close
 */
class HBaseSchema : AbstractSchema {
    private val logger = KotlinLogging.logger {}
    private val connection: Connection
    private val tableMap: MutableMap<String, Table>
    private val statementTableMap: MutableMap<String, MutableMap<String, Table>>

    constructor(connection: Connection) {
        this.connection = connection
        this.tableMap = createTableMap()
        this.statementTableMap = HashMap<String, HashMap<String, Table>>().toMutableMap()
    }

    /**
     * 考虑如果支持create table/index等操作，这里的返回需要去meta获取数据
     */
    override fun getTableMap(): Map<String, Table> {
        return tableMap
    }

    /**
     * 给SqlSchema专用的函数，实现根据连接获取远程的元数据，
     * 每一个新的连接，都会重新去元数据服务器获取一次元数据，这样当分布式环境下，
     * 有数据发生变化的时候，新连接也可以获得当前的信息
     *
     * 我们假设在整个session的过程中，元数据不会变化，如删除、修改某个字段
     * （如果是影响大的DDL使用disableTable迫使session关闭）
     *
     * 这里有个小麻烦，需要定期清理statementId关联的数据，但是目前还没有实现 todo
     */
    @Synchronized
    fun getTableMap(statementId: String): Map<String, Table> {
        if (!statementTableMap.containsKey(statementId)) {
            statementTableMap.put(statementId, createTableMap())
        }
        return statementTableMap.get(statementId)!!
    }

    /**
     * 实现对创建表的支持，在RelNode执行的Context中，包含了CalciteSchema，
     * 可以通过CalciteSchema.schema，获得HBaseSchema的访问，CalciteSchema与HBaseSchema这两者并不一样
     *
     * 需要对RelDataType、RelProtoDataType有深入认识，PrimaryKey通过InitializerExpressionFactory传过来
     *
     * 创建表的流程：
     * 1. 不允许字段名为id的column
     * 2. 在table.sys和column.sys创建对应的记录
     * 3. 检查有没有primary key，如果没有，则需要自动添加id字段，id字段采用uuid的方式生成
     * 4. 根据指定的条件创建相应的表
     * 5. 更新上下文，表的信息登记到本地的Schema中
     */
    fun createTable(name: String,
                    protoStoredRowType: RelProtoDataType,
                    protoRowType: RelProtoDataType,
                    initializerExpressionFactory: InitializerExpressionFactory,
                    keyConstraint: List<String>?,
                    defaultValues: Map<String, Any?>,
                    isTransactional: Boolean): Table {
        val typeFactory = JavaTypeFactoryImpl()

        // 检查column的名字
        val dataType = protoRowType.apply(typeFactory)
        val fieldList = dataType.fieldList
        for (rowType in fieldList) {
            if (rowType.name.compareTo("id", true) == 0)
                throw IllegalColumnNameException("column name should not be id")
        }

        // 在table.sys创建相应的记录
        val put0 = Put(Bytes.toBytes(tableSystableRowkey(name)))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.TABLE_PATH.name), Bytes.toBytes(name))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.IS_TRANSACTIONAL.name), Bytes.toBytes(isTransactional.toString()))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.INDEX_TYPE.name), Bytes.toBytes(HBaseTable.IndexType.NONE.name))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.LOCK_STATUS.name), Bytes.toBytes(HBaseTable.LockStatus.UNLOCK.name))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.CREATE_TIME.name), Bytes.toBytes(LocalDateTime.now().toString()))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.CHARSET.name), Bytes.toBytes(Charsets.UTF_8.toString()))
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.COMMENT.name), Bytes.toBytes(""))
        if (keyConstraint != null) {
            val builder = StringBuilder()
            for (s in keyConstraint) {
                builder.append(s)
            }
            put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.PRIMARY.name), Bytes.toBytes(builder.toString()))
        } else {
            throw PrimaryKeyMissedException("no primary key")
//            put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.PRIMARY.name), Bytes.toBytes(HBaseTable.PrimaryType.UUID.name))
        }
        put0.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.SystemAttribute.INDEX.name), Bytes.toBytes(""))
        val systemhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))
        systemhtable.put(put0)
        systemhtable.close()

        // 在column.sys创建相应的记录, fixme
        val puts = ArrayList<Put>()
        for ((index, rowType) in fieldList.withIndex()) {
            val put1 = Put(Bytes.toBytes(columnSystableRowkey(name, rowType.name)))
            if (initializerExpressionFactory.generationStrategy(null, index) == ColumnStrategy.DEFAULT) {
                val defaultValue = defaultValues.get(rowType.name)
                put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.DEFAULT.name), Bytes.toBytes(defaultValue.toString()))
            } else {
                put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.DEFAULT.name), Bytes.toBytes(null.toString()))
            }
            put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.NULLABLE.name), Bytes.toBytes(rowType.value.isNullable))
            put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.DATA_TYPE.name), Bytes.toBytes(rowType.value.sqlTypeName.toString()))
            put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.PRECISION.name), Bytes.toBytes(rowType.value.precision.toString()))
            put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.POSITION.name), Bytes.toBytes("${index}"))
            put1.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(HBaseTable.ColumnAttribute.COMMENT.name), Bytes.toBytes(""))

            puts.add(put1)
        }
        val columnhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))
        columnhtable.put(puts)
        columnhtable.close()

        // 根据设定的条件创建表
        val admin = connection.admin

        // Instantiating table descriptor class
        val tableDescriptor = HTableDescriptor(TableName.valueOf(name))
        tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
        admin.createTable(tableDescriptor)

        val table = createTable(name, tableDescriptor)

        // 然后添加到tableMap
        tableMap.put(name, table)

        return table
    }

    /**
     * 删除表的流程为：
     * 1. disable表，中断连接上该表的相关连接
     * 2. 对表进行删除
     * 3. 清除在table.sys, column.sys中的相关数据
     * 4. 清除表所在的CalciteSchema信息
     */
    fun dropTable(name: String) {
        val hBaseAdmin = connection.admin
        val tableName = TableName.valueOf(name)

        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        val delete = Delete(Bytes.toBytes(tableSystableRowkey(name)))
        val systemhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))
        systemhtable.delete(delete)
        systemhtable.close()

        // https://blog.csdn.net/tom_fans/article/details/73937578
        // 如果要对rowkey进行过滤，有两种方法，一种是scan1.setFilter(rowfilter)，一种是scan.setRowPrefixFilter
        val scan = Scan()
        scan.setRowPrefixFilter(Bytes.toBytes(name))
        val columnhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))
        val scanner = columnhtable.getScanner(scan)

        val deletes = ArrayList<Delete>()
        for (result in scanner) {
            val del = Delete(result.row)
            deletes.add(del)
        }
        columnhtable.delete(deletes)
        columnhtable.close()

        tableMap.remove(name)
    }

    /**
     * 创建索引的流程：
     * 1. 检查索引是否存在
     * 2. 创建表table.idx.index_name
     * 3. 根据创建语句，从table中获取数据，生成对应的索引记录
     * 4. 更新table对应的table.sys信息
     */
    fun createIndex(indexName: String, indexType: String, tableName: String,
                    keyList: List<String>, isAscList: List<Boolean>) {
        val hBaseAdmin = connection.admin
        val indexTableName = TableName.valueOf(indexTableRowkey(tableName, indexName, HBaseTable.IndexType.valueOf(indexType)))

        if (hBaseAdmin.tableExists(indexTableName)) {
            throw IndexExistsException("${tableName} ${indexName} ${indexType} exists when create index")
        }

        val tableDescriptor = HTableDescriptor(indexTableName)
        tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
        hBaseAdmin.createTable(tableDescriptor)

        // 生成对应的索引记录
        val scan = Scan()
        val sourceTable = HBaseConnection.connection().getTable(TableName.valueOf(tableName))
        val scanner = sourceTable.getScanner(scan)
        val indexTable = HBaseConnection.connection().getTable(indexTableName)
        for (result in scanner) {
            val rowkey = result.row
            val indexkey = StringBuilder()
            for (key in keyList) {
                indexkey.append(result.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(key)))
            }
            val put = Put(Bytes.toBytes(indexkey.toString()))
            put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes("rowkey"), rowkey)
            indexTable.put(put) // 这里考虑有很多条记录的时候，全部读到内存中建立记录再写入，就会有性能问题，todo 考虑分页优化
        }

        // 更新table.sys信息，更新indexType即可，这样每次写入操作的时候即可根据indexType做索引处理
        updateSystableAttribute(tableName, HBaseTable.SystemAttribute.INDEX_TYPE.name, HBaseTable.IndexType.KEY_VALUE.name)
        val index = StringBuilder()
        for (key in keyList) {
            index.append(key)
            index.append(",")
        }
        updateSystableAttribute(tableName, HBaseTable.SystemAttribute.INDEX.name, index.toString())
    }

    /**
     * 删除索引的流程：
     * 1. 检查索引是否存在
     * 2. 修改table.sys，变更索引状态
     * 3. disable索引表
     * 4. 对索引表进行删除
     */
    fun dropIndex(tableName: String, indexName: String,
                  indexType: HBaseTable.IndexType = HBaseTable.IndexType.KEY_VALUE) {
        val hBaseAdmin = connection.admin
        val indexTableName = TableName.valueOf(indexTableRowkey(tableName, indexName, indexType))

        if (!hBaseAdmin.tableExists(indexTableName)) {
            throw IndexExistsException("${tableName} ${indexName} ${indexType} not exists when drop index")
        }

        // 更新table.sys
        updateSystableAttribute(tableName, HBaseTable.SystemAttribute.INDEX_TYPE.name, HBaseTable.IndexType.NONE.name)
        updateSystableAttribute(tableName, HBaseTable.SystemAttribute.INDEX.name, "")

        // disable, delete
        hBaseAdmin.disableTable(indexTableName);
        hBaseAdmin.deleteTable(indexTableName);
    }

    /**
     * 需要支持从HBase中罗列出所有的表
     */
    private fun createTableMap(): MutableMap<String, Table> {
        val admin = connection.admin
        val tables = admin.listTables()

        if (tables.isEmpty()) {
            logger.debug("find none table")
        }

        val builder = HashMap<String, Table>()
        loop@ for (descriptor in tables) {
            val source = descriptor.nameAsString
            when (source) {
                SYSTEM_TABLE_NAME, SYSTEM_COLUMN_NAME -> continue@loop
            }
            val table = createTable(source, descriptor)
            builder.put(source, table)
        }
        val map = builder.toMutableMap()
        return map
    }

    private fun createTable(name: String, descriptor: HTableDescriptor): Table {
        when (HBaseConnection.flavor()) {
            HBaseTable.Flavor.SCANNABLE -> return HBaseScannableTable(name, descriptor)
            HBaseTable.Flavor.FILTERABLE -> return HBaseFilterableTable(name, descriptor)
            HBaseTable.Flavor.PROJECTFILTERABLE -> return HBaseProjectableFilterableTable(name, descriptor)
            else -> throw IllegalArgumentException("Unknown flavor " + HBaseConnection.flavor())
        }
    }

    private fun tableSystableRowkey(table: String): String {
        return table
    }

    private fun columnSystableRowkey(table: String, column: String): String {
        return "${table}.${column}"
    }

    private fun indexTableRowkey(table: String, index: String, indexType: HBaseTable.IndexType): String {
        return "${table}.${indexType.name}.${index}"
    }

    private fun updateSystableAttribute(tableName: String, attribute: String, newValue: String) {
        val get = Get(Bytes.toBytes(tableName))
        val systemhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))

        val dbresult = systemhtable.get(get)
        val rowkey = dbresult.row

        val put = Put(rowkey)
        put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(attribute), Bytes.toBytes(newValue))
        systemhtable.put(put)
        systemhtable.close()
    }
}