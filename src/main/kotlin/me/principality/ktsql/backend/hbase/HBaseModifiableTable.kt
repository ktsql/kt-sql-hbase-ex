package me.principality.ktsql.backend.hbase

import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ModifiableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.hadoop.hbase.HTableDescriptor
import java.lang.reflect.Type

/**
 * https://calcite.apache.org/docs/adapter.html#modifying-data
 *
 * ModifiableTable赋予了Table查询和修改的能力，ModifiableTable继承QueryableTable
 */
abstract class HBaseModifiableTable(name: String, descriptor: HTableDescriptor) :
        HBaseTable(name, descriptor), ModifiableTable {

    /**
     * Returns the element type of the collection that will implement this table
     */
    override fun getElementType(): Type {
        TODO("not implemented")
    }

    /**
     * Queryable继承Enumerable，Schemas.queryable会调用，这个接口的实现来自QueryableTable
     */
    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?,
                                        schema: SchemaPlus?,
                                        tableName: String?): Queryable<T> {
        TODO("not implemented")
    }

    /**
     * 在做计划优化的时候(Prepare.optimize)，需要调用RelOptTableImpl.create，
     * 此时调用getExpression作为参数传给RetOptTableImpl
     */
    override fun getExpression(schema: SchemaPlus?,
                               tableName: String?,
                               clazz: Class<*>?): Expression {
        TODO("not implemented")
    }

    /**
     * 接口要求返回一个可以修改后影响远端服务器数据的MutableCollection对象，
     * 通过封装操作的API是可以做到的
     */
    override fun getModifiableCollection(): MutableCollection<Any?> {
        TODO("not implemented")
    }

    /**
     * 在SqlToRelConverter中被调用，用于获取相关的上下文并保存到RelNode中，
     * 调用TableModify也能对Table进行修改，详看TableModify的实现
     */
    override fun toModificationRel(cluster: RelOptCluster?,
                                   table: RelOptTable?,
                                   catalogReader: Prepare.CatalogReader?,
                                   child: RelNode?,
                                   operation: TableModify.Operation?,
                                   updateColumnList: MutableList<String>?,
                                   sourceExpressionList: MutableList<RexNode>?,
                                   flattened: Boolean): TableModify {
        TODO("not implemented")
    }
}