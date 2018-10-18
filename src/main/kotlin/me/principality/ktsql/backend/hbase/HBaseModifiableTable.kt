package me.principality.ktsql.backend.hbase

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ModifiableTable
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Statistic
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlNode
import org.apache.hadoop.hbase.HTableDescriptor
import java.lang.reflect.Type

/**
 * https://calcite.apache.org/docs/adapter.html#modifying-data
 */
abstract class HBaseModifiableTable(name: String, descriptor: HTableDescriptor) :
        HBaseTable(name, descriptor), ModifiableTable {
    override fun getModifiableCollection(): MutableCollection<Any?> {
        TODO("not implemented")
    }

    override fun getElementType(): Type {
        TODO("not implemented")
    }

    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?, schema: SchemaPlus?, tableName: String?): Queryable<T> {
        TODO("not implemented")
    }

    override fun getExpression(schema: SchemaPlus?, tableName: String?, clazz: Class<*>?): Expression {
        TODO("not implemented")
    }

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