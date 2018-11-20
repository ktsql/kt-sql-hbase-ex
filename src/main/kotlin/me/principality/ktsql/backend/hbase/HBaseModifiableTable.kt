package me.principality.ktsql.backend.hbase

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ModifiableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.filter.*
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
         * 这里要考虑好如何表达需要修改的值，需要传进来的是一行的有效数据，按顺序排序
         * 如果需要判断主键(rowkey)是否重复，还需要从远程读一次元数据，以便对插入的数据进行检查
         *
         * todo 需要对默认值、是否为空进行判断？
         *
         * 数据在插入前做了格式和值的对应检查，参考validateInsert
         */
        override fun add(element: Any?): Boolean {
            val values = element as Array<Any>

            for ((index, columnDef) in columnDescriptors.withIndex()) {
                if (columnDef.isPrimary) {
                    val rowkey = values.get(index).toString()
                    val put = Put(Bytes.toBytes(rowkey))

                    // todo 使用typeFactory对col进行处理？
//                    val typeFactory = JavaTypeFactoryImpl()
                    for ((idx, col) in values.withIndex()) {
//                        val dataType = typeFactory.createSqlType(SqlTypeName.get(columnDescriptors.get(idx).type))
//                        val javaType = typeFactory.getJavaClass(dataType)
                        if (idx != index) {
                            val target = values.get(idx).toString()
                            put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(columnDescriptors.get(idx).name), Bytes.toBytes(target))
                        }
//                        when (col.type) {
//                            "INTEGER" -> {
//                                target = col as String
//                                put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(rowkey), Bytes.toBytes(target))
//                            }
//                            "VARCHAR" -> {
//                                target = col as String
//                                put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(rowkey), Bytes.toBytes(target))
//                            }
//                            else -> target = null
//                        }
                    }

                    val htable = getHTable(name)
                    htable.put(put)
                    htable.close()
                    return true
                }
            }
            return false
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
         * hbase是以rowkey，column，timestamp这三个维度来区分的，
         * 传进来的Any?应可描述rowkey, column, timestamp
         * 在timestamp不暴露的前提下，依赖rowkey和column对数据进行定位
         *
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

    /**
     * 为子类提供扫描全表的实现
     */
    protected fun scan(): Enumerable<Array<Any>> {
        val connection = HBaseConnection.connection()
        val htable = connection.getTable(TableName.valueOf(name))

        val scan = Scan()
        var rs: ResultScanner? = null
        try {
            rs = htable.getScanner(scan)
            return SqlEnumerableImpl<Array<Any>>(rs)
        } finally {
            rs!!.close()
            htable.close()
        }
    }

    /**
     * 支持 =, <, >, >=, <=，如 op > rop，其中op用$2表示是表中columndef.get(2)，
     * 也可以用op.index返回所对应的column
     */
    protected fun translateMatch2(node: RexNode): Filter {
        when (node.kind) {
            SqlKind.EQUALS -> return translateBinary("=", "=", node as RexCall)
            SqlKind.LESS_THAN -> return translateBinary("<", ">", node as RexCall)
            SqlKind.LESS_THAN_OR_EQUAL -> return translateBinary("<=", ">=", node as RexCall)
            SqlKind.GREATER_THAN -> return translateBinary(">", "<", node as RexCall)
            SqlKind.GREATER_THAN_OR_EQUAL -> return translateBinary(">=", "<=", node as RexCall)
            else -> throw AssertionError("cannot translate $node")
        }
    }

    protected fun translateBinary(op: String, rop: String, call: RexCall): Filter {
        val left = call.operands[0]
        val right = call.operands[1]
        var expression = translateBinary2(op, left, right)
        if (expression != null) {
            return expression
        }
        expression = translateBinary2(rop, right, left)
        if (expression != null) {
            return expression
        }
        throw AssertionError("cannot translate op $op call $call")
    }

    /**
     * 这里对输入值进行检查并转换
     */
    protected fun translateBinary2(op: String, left: RexNode, right: RexNode): Filter? {
        if (right.kind != SqlKind.LITERAL) {
            return null
        }
        val rightLiteral = right as RexLiteral

        when (left.kind) {
            SqlKind.INPUT_REF -> {
                val leftRef = left as RexInputRef
                return translateOp2(op, leftRef, rightLiteral)
            }
            SqlKind.CAST ->
                return translateBinary2(op, (left as RexCall).operands[0], right)
            else -> return null
        }
    }

    /**
     * 最后的filter在这里产生
     */
    protected fun translateOp2(op: String, leftRef: RexInputRef, right: RexLiteral): Filter? {
        val compareOp: CompareFilter.CompareOp
        when (op) {
            "=" -> compareOp = CompareFilter.CompareOp.EQUAL
            ">" -> compareOp = CompareFilter.CompareOp.GREATER
            "<" -> compareOp = CompareFilter.CompareOp.LESS
            ">=" -> compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL
            "<=" -> compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL
            else ->
                return null
        }

        val value = literalValue(right)

        // 主键，用rowfilter，否则用SingleColumnValueFilter
        if (this.columnDescriptors.get(leftRef.index).isPrimary) {
            return RowFilter(compareOp, BinaryComparator(Bytes.toBytes(value)))
        } else {
            return SingleColumnValueFilter(Bytes.toBytes(columnFamily),//列族
                    Bytes.toBytes(this.columnDescriptors.get(leftRef.index).name),  //列名
                    compareOp, Bytes.toBytes(value))
        }
    }

    private fun literalValue(literal: RexLiteral): String {
        val value = literal.value2
        val buf = StringBuilder()
        buf.append(value)
        return buf.toString()
    }
}