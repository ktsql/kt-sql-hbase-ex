package me.principality.ktsql.backend.hbase

import java.util.*
import java.util.UUID.randomUUID


object HBaseUtils {
    val SYSTEM_TABLE_NAME = "table.sys"
    val SYSTEM_COLUMN_NAME = "column.sys"

    /**
     * 根据参数返回indexTable的名称
     */
    fun indexTableName(table: String, indexName: String, indexType: String): String {
        return "{$table.$indexType.$indexName}"
    }

    /**
     * uuid 32bit
     */
    fun generatePrimaryId(): String {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase()
    }
}