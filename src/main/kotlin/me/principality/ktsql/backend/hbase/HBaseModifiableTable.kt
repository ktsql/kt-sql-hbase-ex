package me.principality.ktsql.backend.hbase

import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ModifiableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas
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
 * Queryable继承Enumerable
 *
 * 在查询(select)时，有两种查询数据的方法，分别是：Schemas.queryable和Schemas.enumerable
 *
 * Schemas.queryable会调用table.asQueryable，把需要查询的数据转换成Enumerable，
 * 这时的操作，需要table adaptor实现接口table.asQueryable
 *
 * Schemas.enumerable会调用table.scan，把数据从table中读出来，这时的操作，
 * 需要table adaptor实现接口table.scan
 *
 * 具体采用哪种(queryable/enumerable)转换的方式，是在RelOptTableImpl.getClassExpressionFunction中实现的，
 * 做计划优化的时候(Prepare.optimize)，RelNode.optimize会调用RelOptTableImpl.create，
 * RelOptTableImpl.create会调用RelOptTableImpl.getClassExpressionFunction来完成转化，
 * queryable此时调用getExpression作为参数传给RetOptTableImpl，而enumerable调用Schemas.tableExpression
 *
 * 在修改时(insert/update/delete)时，参考：
 * http://ktsql.github.io/2018/11/16/Calcite的ModificationRel/
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
     * 并且在转换成执行的binary code时，调用table.getModifiableCollection()
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

    /**
     * 如果在这里重写了getExpression，就不用hack RelOptTableImpl.getClassExpressionFunction，
     */
    override fun getExpression(schema: SchemaPlus, tableName: String,
                               clazz: Class<*>): Expression {
        return Schemas.tableExpression(schema, elementType, tableName, clazz)
    }

    /**
     * 返回一个内部类，可以访问所在类的上下文，按需调用
     */
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
         * 如果是按顺序排序的话，还需要从原程读一次元数据，以便对插入的顺序进行处理
         *
         * 数据在插入前做了格式和值的对应检查，参考validateInsert
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