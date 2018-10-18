package me.principality.ktsql.backend.hbase

import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.lang.IllegalArgumentException

/**
 * https://calcite.apache.org/docs/tutorial.html
 *
 * 一个完整的calcite adapter实现，需要包括：
 * 1. SchemaFactory 获取输入参数，完成环境的初始化，并创建Schema
 * 2. Schema 负责创建表，依据SchemaFactory提供的上下文进行表的初始化
 * 3. Table 表的访问接口，功能的实现者
 *
 * 完成以下初始化功能：
 * 1. 完成与后端连接的初始化
 * 2. 把后端连接传递给HBaseSchema
 *
 * Schema的创建是一次性的，把需要保存的上下文放到Schema中
 */
class HBaseSchemaFactory : SchemaFactory {
    override fun create(parentSchema: SchemaPlus?,
                        name: String?,
                        operand: MutableMap<String, Any>?): Schema {
        if (name != "HBASE") {
            throw IllegalArgumentException("properties schemas name error")
        }

        if (operand == null || operand.isEmpty()) {
            throw IllegalArgumentException("properties schemas operand error")
        }

        HBaseConnection.init(operand)
        return HBaseSchema(HBaseConnection.connection())
    }
}