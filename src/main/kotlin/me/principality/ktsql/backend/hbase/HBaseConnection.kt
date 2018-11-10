package me.principality.ktsql.backend.hbase

import org.apache.calcite.schema.Table
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.util.*

/**
 * 统一管理HBaseConnection，避免重复创建，多个session可以共享一个connection
 *
 * Calcite在资源使用完以后，对连接资源进行管理（如关闭）。使用jdbc对存储层读取进行包装后，
 * 调用层(如SQL Execute)无法对底层进行管理，必须要通过backend暴露出底层的资源管理接口，
 * 才能对存储层连接资源进行管理
 *
 * 具体的资源管理机制，与HBase的客户端逻辑相关
 * https://blog.csdn.net/jediael_lu/article/details/76619000
 */
object HBaseConnection {
    private var isInit = false
    private lateinit var connection: Connection
    private lateinit var flavor: HBaseTable.Flavor

    /**
     * 初始化的流程：
     * 1. 创建连接
     * 2. 检查系统表是否存在(table.sys, column.sys)
     */
    fun init(operand: MutableMap<String, Any>?) {
        val config = HBaseConfiguration.create()
        val zkquorum: String = operand?.get("zkquorum").toString()
        config.set(HConstants.ZOOKEEPER_QUORUM, zkquorum)

        isInit = true
        val flavorName = operand?.get("flavor").toString()
        if (flavorName == null) {
            flavor = HBaseTable.Flavor.SCANNABLE
        } else {
            flavor = HBaseTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT))
        }
        connection = ConnectionFactory.createConnection(config)

        confirmTableExists(HBaseUtils.SYSTEM_TABLE_NAME)
        confirmTableExists(HBaseUtils.SYSTEM_COLUMN_NAME)
    }

    fun connection(): Connection {
        if (!isInit)
            throw RuntimeException("call connection before init")

        return connection
    }

    fun close() {
        if (isInit) {
            connection.close()
            isInit = false
        }
    }

    fun flavor(): HBaseTable.Flavor {
        if (isInit) {
            return flavor
        }
        return HBaseTable.Flavor.SCANNABLE
    }

    /**
     * 确认表是否存在，如果不存在则生成
     */
    private fun confirmTableExists(name: String) {
        fun confirmSystemTable() {
            val admin = connection.admin

            if (admin.tableExists(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))) {
                return
            }

            val tableDescriptor = HTableDescriptor(TableName.valueOf(HBaseUtils.SYSTEM_TABLE_NAME))
            tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
            admin.createTable(tableDescriptor)
        }

        fun confirmColumnTable() {
            val admin = connection.admin

            if (admin.tableExists(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))) {
                return
            }

            val tableDescriptor = HTableDescriptor(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))
            tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
            admin.createTable(tableDescriptor)
        }

        when (name) {
            HBaseUtils.SYSTEM_TABLE_NAME -> confirmSystemTable()
            HBaseUtils.SYSTEM_COLUMN_NAME -> confirmColumnTable()
            else -> throw IllegalArgumentException("check no exists system table")
        }
    }
}