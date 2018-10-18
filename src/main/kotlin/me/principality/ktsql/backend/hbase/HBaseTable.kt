package me.principality.ktsql.backend.hbase

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.util.Pair
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table
import java.util.ArrayList
import org.apache.hadoop.hbase.HColumnDescriptor



/**
 * 通过实现不同类型的表，优化查询性能
 * https://calcite.apache.org/docs/tutorial.html#optimizing-queries-using-planner-rules
 *
 * org.apache.calcite.schema 定义了几种常见的表
 *   org.apache.calcite.schema.ExtensibleTable
 *   org.apache.calcite.schema.FilterableTable
 *   org.apache.calcite.schema.ModifiableTable
 *   org.apache.calcite.schema.ProjectableFilterableTable
 *   org.apache.calcite.schema.QueryableTable
 *   org.apache.calcite.schema.ScannableTable
 *   org.apache.calcite.schema.StreamableTable
 *   org.apache.calcite.schema.TranslatableTable
 *
 * 关注：ScannableTable, FilterableTable, ProjectableFilterableTable, TranslatableTable
 *
 * adapter负责对接calcite，最终的实现通过hbase-client来完成，如果要实现计算下推，采用TranslatableTable
 *
 * 如果使用HBase作为后端存储，每个表的数据都可能存放在多个region上，
 * 要访问每个表的数据，则需要维系表对应的region list，并根据数据的获取需求，
 * 建立和多个region server的连接
 *
 * HBase加速查询的关键技术点：
 * 1. 通过多个region server实现数据并行读取，通过rowkey高效完成数据range判断
 * 2. 如果使用filter读取数据，可以在region server端完成数据过滤
 * 3. 通过压缩传输减少数据传输所需的时间（可以考虑结合压缩算法实现更高效的SQL查询）
 * 4. 部分aggregate处理可以下推到region server完成（需要与calcite配合实现）
 *
 * 读取数据的速度取决于region server和网络速度，配上万兆网卡或者RDMA等网络设备可有效提升速度
 *
 * SQL查询应尽量考虑把project, filter以及aggregate下推到region server，从而获得更好的性能
 */
abstract class HBaseTable : AbstractTable {
    protected val name: String
    protected val htableDescriptor: HTableDescriptor
    protected val table: Table

    constructor(name: String, descriptor: HTableDescriptor) {
        this.name = name
        this.htableDescriptor = descriptor
        this.table = HBaseConnection.connection().getTable(TableName.valueOf(name))
    }

    /**
     * 通过该函数获得the names and types of a table's columns
     *
     * 可以通过hbase的表接口获得该信息，需要注意的是：
     * 如何实现从不同数据源数据类型映射/转换到Calcite数据类型的逻辑？
     *
     * HBase的column是不区分类型的
     */
    override fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
        if (typeFactory == null) {
            throw IllegalArgumentException("RelDataTypeFactory is null")
        }

        val types = ArrayList<RelDataType>()
        val names = ArrayList<String>()

        // 遍历获得每一个column的类型，借助HColumnDescriptor
        val columnDescriptors = htableDescriptor.getColumnFamilies()

        for (column in columnDescriptors) {
            val javaType = typeFactory.createJavaType(String::class.java)
            val sqlType = typeFactory.createSqlType(javaType.sqlTypeName)

            names.add(column.nameAsString)
            types.add(sqlType)
        }

        return typeFactory.createStructType(Pair.zip(names, types))
    }

    /**
     * 对表的数据读取方式进行定义，
     * - 如果是全表扫描，过滤和投影由calcite完成
     * - 如果是过滤扫描，投影由calcite完成
     * - 如果是投影过滤，则calcite没有额外的工作
     *
     * 实现三种模式的处理，可以对性能进行比较测试
     */
    enum class Flavor {
        SCANNABLE, FILTERABLE, PROJECTFILTERABLE
    }
}