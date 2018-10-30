package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.FilterableTable
import org.apache.hadoop.hbase.HTableDescriptor

class HBaseFilterableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), FilterableTable {

    /**
     * hbase是以rowkey，column，timestamp这三个维度来区分的，
     * 传进来的Any?应可描述rowkey, column, timestamp
     * 在timestamp不暴露的前提下，依赖rowkey和column对数据进行定位
     *
     * 如果是依据rowkey获取的模式，则按rowkey获取，
     * 如果是范围scan，则按范围获取的方式返回
     */
    override fun scan(root: DataContext?,
                      filters: MutableList<RexNode>?): Enumerable<Array<Any>> {
        TODO("not implemented")

        /*
        // 通过scan扫描全表数据，scan中可以加入一些过滤条件
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("user"));
        scan.setStopRow(Bytes.toBytes("zk002"));
        scan.setTimeRange(1488252774189l, 1488252774191l);
        ResultScanner resultScann1 = hbase.getResultScann(TABLE_NAME, scan);
        printResultScanner(resultScann1);
         */
    }
}