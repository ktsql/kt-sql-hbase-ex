package me.principality.ktsql.backend.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.util.*

/**
 * 统一管理HBaseConnection，避免重复创建
 *
 * Calcite并没有对连接资源进行管理（如关闭），且使用jdbc对存储层读取进行包装后，
 * 调用层(如SQL Execute)也无法对底层进行管理，必须要通过backend暴露出底层的资源管理接口，
 * 才能对存储层连接资源进行管理
 *
 * 具体的资源管理机制，与HBase的客户端逻辑相关
 * https://blog.csdn.net/jediael_lu/article/details/76619000
 */
object HBaseConnection {
    private var isInit = false
    private lateinit var connection: Connection
    private lateinit var flavor: HBaseTable.Flavor

    fun init(operand: MutableMap<String, Any>?) {
        val config = HBaseConfiguration.create()
        val zkquorum: String = operand?.get("zkquorum").toString()
        config.set(HConstants.ZOOKEEPER_QUORUM, zkquorum)

        isInit = true
        connection = ConnectionFactory.createConnection(config)
        val flavorName = operand?.get("flavor").toString()
        if (flavorName == null) {
            flavor = HBaseTable.Flavor.SCANNABLE
        } else {
            flavor = HBaseTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT))
        }
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
}