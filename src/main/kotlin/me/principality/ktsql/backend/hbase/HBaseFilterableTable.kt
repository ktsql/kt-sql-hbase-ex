package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.FilterableTable
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.filter.*
import org.apache.hadoop.hbase.util.Bytes


class HBaseFilterableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), FilterableTable {

    /**
     * 如果是依据rowkey获取的模式，则按rowkey获取，
     * 如果是范围scan，则按范围获取的方式返回
     *
     * RexNode用于表达filters
     */
    override fun scan(root: DataContext?,
                      filters: MutableList<RexNode>?): Enumerable<Array<Any>> {
        if (filters != null) {
            val htable = getHTable(this.name)
            val scan = Scan()
            val filterList = FilterList(FilterList.Operator.MUST_PASS_ALL)

            for (filter in filters) {
                val f = translateMatch2(filter)
                filterList.addFilter(f)
            }
            scan.setFilter(filterList)
            val rs = htable.getScanner(scan)
            return SqlEnumerableImpl<Array<Any>>(rs)
        } else {
            return scan()
        }
    }
}