package me.principality.ktsql.backend.hbase

import org.junit.Test

class EnumTest {

    @Test
    fun enumTest () {
        val name = HBaseTable.IndexType.NONE.name.toString()
        val type = HBaseTable.IndexType.valueOf(name)
        println(name)
        println(type)
    }
}