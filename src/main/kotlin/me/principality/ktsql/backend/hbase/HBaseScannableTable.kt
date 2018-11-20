package me.principality.ktsql.backend.hbase

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTableQueryable
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan

/**
 * 实现对扫描的支持，创建时，初始化相关环境，在scan()时完成对数据的扫描处理
 */
class HBaseScannableTable(name: String, descriptor: HTableDescriptor) :
        HBaseModifiableTable(name, descriptor), ScannableTable {

    /**
     * 实现全表扫描的数据记录读取
     */
    override fun scan(root: DataContext?): Enumerable<Array<Any>> {
        return scan()
    }

    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?,
                                        schema: SchemaPlus?,
                                        tableName: String?): Queryable<T> {
        return object : AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            override fun enumerator(): Enumerator<T> {
                val enumerable = scan() as Enumerable<T>
                return enumerable.enumerator()
            }
        }
    }
}