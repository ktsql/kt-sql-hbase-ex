package me.principality.ktsql.backend.hbase

import me.principality.ktsql.backend.hbase.utils.ConfigProvider
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

        val zkquorum: String = ConfigProvider.zkquorum
        val flavor = ConfigProvider.flavor

        map.put("zkquorum", zkquorum)
        map.put("flavor", flavor)

        return map
    }
}