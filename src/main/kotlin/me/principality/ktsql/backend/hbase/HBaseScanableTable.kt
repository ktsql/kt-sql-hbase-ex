package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.schema.ScannableTable
import org.apache.hadoop.hbase.HTableDescriptor


/**
 * 实现对扫描的支持，创建时，初始化相关环境，在scan()时完成对数据的扫描处理
 */
class HBaseScanableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), ScannableTable {

    /**
     *
     */
    override fun scan(root: DataContext?): Enumerable<Array<Any>> {
        TODO("not implemented")

/*
        val scan = Scan()
        var rs: ResultScanner? = null
        val htable = HTable(conf, tableName)
        try {
            rs = htable.getScanner(scan)
            for (r in rs!!) {
                for (kv in r.list()) {

                    System.out.println(Bytes.toString(kv.getRow()))
                    System.out.println(Bytes.toString(kv.getFamily()))
                    System.out.println(Bytes.toString(kv.getQualifier()))
                    System.out.println(Bytes.toString(kv.getValue()))
                    System.out.println(kv.getTimestamp())
                }
            }
        } finally {
            rs!!.close()
        }
        return rs
*/
    }
}