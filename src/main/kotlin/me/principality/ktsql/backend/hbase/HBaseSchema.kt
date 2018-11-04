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
     * 可以通过CalciteSchema.schema，获得HBaseSchema的访问，这两者并不一样
     *
     * 需要对RelDataType、RelProtoDataType有深入认识
     *
     * 创建表的流程：
     * 1. 在table.sys和column.sys创建对应的记录
     * 2. 根据指定的条件创建相应的表
     * 3. 更新上下文，表的信息登记到本地的Schema中
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

    /**
     * 删除表的流程为：
     * 1. disable表，中断连接上该表的相关连接
     * 2. 对表进行删除
     * 3. 清除在table.sys中的相关数据
     */
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

        if (tables.isEmpty()) {
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