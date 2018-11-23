package me.principality.ktsql.backend.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.*
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.OrderedBytes.UTF8
import org.junit.Test
import java.nio.charset.Charset

class HBaseTest {

    @Test
    fun testValueFilter() {
        val htable = HBaseConnection.connection().getTable(TableName.valueOf("T"))
        val scan = Scan()
        val filterList = FilterList(FilterList.Operator.MUST_PASS_ALL)

        // 测试扫描的过滤条件
        val f = SingleColumnValueFilter(Bytes.toBytes(HBaseTable.columnFamily),//列族
                Bytes.toBytes("POS"),  //列名
                CompareFilter.CompareOp.GREATER, Bytes.toBytes(0))
        filterList.addFilter(f)
        scan.setFilter(filterList)
        val rs = htable.getScanner(scan)
        for (result in rs) {
            println("${result.row.toString(Charset.defaultCharset())}")
        }
        println(rs)
    }

    @Test
    fun testRowFilter() {
        val htable = HBaseConnection.connection().getTable(TableName.valueOf("T"))
        val scan = Scan()
        val filterList = FilterList(FilterList.Operator.MUST_PASS_ALL)

        // 测试扫描的过滤条件
        val f = RowFilter(CompareFilter.CompareOp.EQUAL, BinaryComparator(Bytes.toBytes("XXXX")))
        filterList.addFilter(f)
        scan.setFilter(filterList)
        val rs = htable.getScanner(scan)
        for (result in rs) {
            println("${result.row.toString(Charset.defaultCharset())}")
        }
        println(rs)
    }

    @Test
    fun testRowFilter2() {
        HBaseConnection.init(fakeOperands())

        val scan = Scan()
        scan.setRowPrefixFilter(Bytes.toBytes("T"))
        val columnhtable = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.SYSTEM_COLUMN_NAME))
        val scanner = columnhtable.getScanner(scan)

        for (result in scanner) {
            val row = result.row.toString(UTF8)
            println(row)
        }
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