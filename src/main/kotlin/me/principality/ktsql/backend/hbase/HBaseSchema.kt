package me.principality.ktsql.backend.hbase

import mu.KotlinLogging
import org.apache.calcite.rel.type.RelProtoDataType
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql2rel.InitializerExpressionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection

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
 * TODO calcite没有释放connection, admin, htable的机制
 */
class HBaseSchema : AbstractSchema {
    private val logger = KotlinLogging.logger {}
    private val connection: Connection
    private val tableMap: MutableMap<String, Table>

    constructor(connection: Connection) {
        this.connection = connection
        this.tableMap = createTableMap()
    }

    /**
     * 考虑如果支持create table/index等操作，这里的返回需要去meta获取数据
     */
    override fun getTableMap(): Map<String, Table> {
        return tableMap
    }

    /**
     * 实现对创建表的支持，在RelNode执行的Context中，包含了CalciteSchema，
     * 可以通过CalciteSchema.schema，获得HBaseSchema的访问，这两者并不一样
     *
     * 需要对RelDataType、RelProtoDataType有深入认识
     */
    fun createTable(name: String,
                    protoStoredRowType: RelProtoDataType,
                    protoRowType: RelProtoDataType,
                    initializerExpressionFactory: InitializerExpressionFactory): Table {
        // 先创建表
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

    fun dropTable(name: String) {
        val hBaseAdmin = connection.admin
        val tableName = TableName.valueOf(name)

        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }

    /**
     * 需要支持从HBase中罗列出所有的表
     */
    private fun createTableMap(): MutableMap<String, Table> {
        val admin = connection.admin
        val tables = admin.listTables()

        if (tables.size == 0) {
            logger.debug("find none table")
        }

        val builder = HashMap<String, Table>()
        for (descriptor in tables) {
            val source = descriptor.nameAsString
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
}