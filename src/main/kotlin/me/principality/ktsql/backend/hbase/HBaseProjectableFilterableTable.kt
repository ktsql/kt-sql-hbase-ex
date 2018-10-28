package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.hadoop.hbase.HTableDescriptor

class HBaseProjectableFilterableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), ProjectableFilterableTable {

    override fun scan(root: DataContext?,
                      filters: MutableList<RexNode>?,
                      projects: IntArray?): Enumerable<Array<Any>> {
        TODO("not implemented")

        /*
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("salary"),
                Bytes.toBytes("gross"),
                CompareOp.GREATER,
                Bytes.toBytes("1500")
        );

        //To prevent the entire row from being emitted
        //if the column is not found on a row
        scan.setFilterIfMissing(true)
        scan.setFilter(filter);

        scan.addFamily(Bytes.toBytes("ename"))
        scan.addColumn(Bytes.toBytes("salary"), Bytes.toBytes("da"))
        scan.addColumn(Bytes.toBytes("salary"), Bytes.toBytes("gross"))
        */
    }
}