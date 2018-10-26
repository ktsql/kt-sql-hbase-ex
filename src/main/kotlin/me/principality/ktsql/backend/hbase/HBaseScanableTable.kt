package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.schema.ScannableTable
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan

/**
 * 实现对扫描的支持，创建时，初始化相关环境，在scan()时完成对数据的扫描处理
 */
class HBaseScanableTable(name: String, descriptor: HTableDescriptor) :
        HBaseTable(name, descriptor), ScannableTable {

    /**
     * 实现全表扫描的数据记录读取
     */
    override fun scan(root: DataContext?): Enumerable<Array<Any>> {
        val connection = HBaseConnection.connection()
        val htable = connection.getTable(TableName.valueOf(name))

        val scan = Scan()
        var rs: ResultScanner? = null
        try {
            rs = htable.getScanner(scan)
            val array = Array<Any>(rs.count(), {})
            for ((k, v) in rs!!.withIndex()) {
                array.set(k, v)
            }
            return EnumerableImpl<Array<Any>>(rs)
        } finally {
            rs!!.close()
        }
    }
}