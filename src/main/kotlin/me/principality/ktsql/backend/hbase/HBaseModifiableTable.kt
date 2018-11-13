package me.principality.ktsql.backend.hbase

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ModifiableTable
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.util.Bytes
import java.util.*


/**
 * https://calcite.apache.org/docs/adapter.html#modifying-data
 *
 * ModifiableTable赋予了Table查询和修改的能力，ModifiableTable继承QueryableTable
 * Queryable继承Enumerable，Schemas.queryable会调用，这个接口的实现来自QueryableTable
 * 在做计划优化的时候(Prepare.optimize)，需要调用RelOptTableImpl.create，
 * 此时调用getExpression作为参数传给RetOptTableImpl
 */
abstract class HBaseModifiableTable(name: String, descriptor: HTableDescriptor) :
        HBaseTable(name, descriptor), ModifiableTable {
    /**
     * 接口要求返回一个可以修改后影响远端服务器数据的MutableCollection对象，
     * 通过封装操作的API是可以做到的
     */
    override fun getModifiableCollection(): MutableCollection<Any?> {
        return HBaseMutableCollection()
    }

    /**
     * 在SqlToRelConverter中被调用，用于获取相关的上下文并保存到RelNode中，
     * Interpreter调用TableModify最终会转换为EnumerableTableModify，
     * 并且在转换成执行的binary code时，调用getModifiableCollection()
     */
    override fun toModificationRel(cluster: RelOptCluster?,
                                   table: RelOptTable?,
                                   catalogReader: Prepare.CatalogReader?,
                                   child: RelNode?,
                                   operation: TableModify.Operation?,
                                   updateColumnList: MutableList<String>?,
                                   sourceExpressionList: MutableList<RexNode>?,
                                   flattened: Boolean): TableModify {
        return LogicalTableModify(cluster, cluster?.traitSetOf(Convention.NONE),
                table, catalogReader, child, operation, updateColumnList,
                sourceExpressionList, flattened)
    }

    inner class HBaseMutableCollection(override val size: Int = 0) : MutableCollection<Any?> {

        override fun contains(element: Any?): Boolean {
            TODO("not implemented")
        }

        override fun containsAll(elements: Collection<Any?>): Boolean {
            TODO("not implemented")
        }

        override fun isEmpty(): Boolean {
            TODO("not implemented")
        }

        /**
         * insert调用的是add()，参考asEnumerable.into
         * 这里要考虑好如何表达需要修改的值，需要传进来的是一行的有效数据
         */
        override fun add(element: Any?): Boolean {
            val target = element as String
            val put = Put(Bytes.toBytes("rowkey"))
            put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes("rowkey"), Bytes.toBytes(target))
            val htable = getHTable(name)
            htable.put(put)
            htable.close()
            return true
        }

        override fun addAll(elements: Collection<Any?>): Boolean {
            TODO("not implemented")
        }

        override fun clear() {
            TODO("not implemented")
        }

        override fun iterator(): MutableIterator<Any?> {
            TODO("not implemented")
        }

        override fun remove(element: Any?): Boolean {
            TODO("not implemented")
        }

        /**
         * delete调用的是removeAll操作
         * removeAll是通过rowkey的方式来表达，还是通过范围处理来表达？
         */
        override fun removeAll(elements: Collection<Any?>): Boolean {
            val deletes = ArrayList<Delete>()
            val targets = elements as Collection<String> // fixme hack cast
            for (target in targets) {
                val delete = Delete(Bytes.toBytes(target))
                deletes.add(delete)
            }
            val htable = getHTable(name)
            htable.delete(deletes)
            htable.close()
            return true
        }

        override fun retainAll(elements: Collection<Any?>): Boolean {
            TODO("not implemented")
        }

        private fun get(get: Get): Collection<Row> {
            TODO()
        }

        private fun convert(collection: Collection<Any?>): Get {
            TODO()
        }
    }
}