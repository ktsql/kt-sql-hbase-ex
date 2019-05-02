package me.principality.ktsql.backend.hbase

import org.junit.Test

/**
 * 1. 测试index保存在hbase中
 * 2. 测试扩展的功能是否正常
 */
class LuceneTest {
    @Test
    fun testSaving() {
        val map = fakeOperands()

    }

    @Test
    fun testDictionary() {

    }

    private fun fakeOperands(): MutableMap<String, Any> {
        val map = HashMap<String, Any>()

        val zkquorum: String = "127.0.0.1:2222"
        val flavor = "FILTERABLE"

        map.put("zkquorum", zkquorum)
        map.put("flavor", flavor)

        return map
    }
}