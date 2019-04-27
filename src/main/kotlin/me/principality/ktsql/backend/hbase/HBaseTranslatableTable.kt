package me.principality.ktsql.backend.hbase

import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.QueryableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.TranslatableTable
import org.apache.hadoop.hbase.HTableDescriptor
import java.lang.reflect.Type

/**
 * 未实现，下推算法如何做要进一步考虑
 */
@Deprecated("")
class HBaseTranslatableTable: HBaseTable, QueryableTable, TranslatableTable {
    constructor(name: String, htable: HTableDescriptor) : super(name, htable) {
    }

    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?,
                                        schema: SchemaPlus?, tableName: String?): Queryable<T> {
        TODO("not implemented")
    }

    override fun getElementType(): Type {
        TODO("not implemented")
    }

    override fun getExpression(schema: SchemaPlus?, tableName: String?, clazz: Class<*>?): Expression {
        TODO("not implemented")
    }

    override fun toRel(context: RelOptTable.ToRelContext?, relOptTable: RelOptTable?): RelNode {
        TODO("not implemented")
    }
}