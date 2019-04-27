package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.util.Bytes

class HBaseProjectableFilterableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), ProjectableFilterableTable {

    override fun scan(root: DataContext?,
                      filters: MutableList<RexNode>?,
                      projects: IntArray?): Enumerable<Array<Any>> {
        if (filters != null) {
            val htable = getHTable(this.name)
            val scan = Scan()
            val filterList = FilterList(FilterList.Operator.MUST_PASS_ALL)

            for (filter in filters) {
                val f = translateMatch2(filter)
                filterList.addFilter(f)
            }

            if (projects != null) {
                for (project in projects) {
                    val name = this.columnDescriptors.get(project).name
                    scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(name)) // 选择特定列
                }
            }

            scan.setFilter(filterList)
            val rs = htable.getScanner(scan)
            return SqlEnumerableImpl(rs)
        } else {
            return scan()
        }
    }
}