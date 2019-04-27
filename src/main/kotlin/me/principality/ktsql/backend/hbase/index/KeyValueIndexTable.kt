package me.principality.ktsql.backend.hbase.index

import com.google.common.base.Throwables
import me.principality.ktsql.backend.hbase.HBaseConnection
import me.principality.ktsql.backend.hbase.HBaseTable
import me.principality.ktsql.backend.hbase.HBaseUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes
import java.io.Closeable
import java.io.IOException
import java.util.*

/**
 * 二次索引实现，这是一个不包含事务的实现版本，作为早期版本测试用
 */
class KeyValueIndexTable : Closeable {
    private val sourceHTable: Table
    private val indexHTable: Table
    private val secondaryIndexTableName: TableName
    private val hbaseConnection: Connection
    private val secondaryIndexColumn: ByteArray

    @Throws(IOException::class)
    constructor(table: Table,
                columnName: ByteArray,
                indexName: String,
                indexType: String = HBaseTable.IndexType.KEY_VALUE.name) {
        secondaryIndexTableName =
                TableName.valueOf(HBaseUtils.indexTableName(table.name.nameAsString, indexName, indexType))
        this.hbaseConnection = HBaseConnection.connection()
        this.secondaryIndexColumn = columnName
        var secondaryIndexHTable: Table? = null
        try {
            this.hbaseConnection.admin.use { hBaseAdmin ->
                if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
//                    val tableDescriptor = HTableDescriptor(secondaryIndexTableName)
//                    tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
//                    hBaseAdmin.createTable(tableDescriptor)
                    hBaseAdmin.createTable(TableDescriptorBuilder.newBuilder(secondaryIndexTableName).build())
                }
                secondaryIndexHTable = this.hbaseConnection.getTable(secondaryIndexTableName)
            }
        } catch (e: Exception) {
            Throwables.propagate(e)
        }

        this.sourceHTable = table
        this.indexHTable = secondaryIndexHTable!!
    }

    companion object {
        private val secondaryIndexFamily = Bytes.toBytes("sif")
        private val secondaryIndexQualifier = Bytes.toBytes("r")
        private val DELIMITER = byteArrayOf(0)
    }

    /**
     * 根据特定值（必须相等），从二次索引获取相应的rowkey，这里可以修改为filter的方式
     */
    @Throws(IOException::class)
    fun getByIndex(qualifier: ByteArray, value: ByteArray): Array<Result>? {
        try {
            val key = Bytes.add(qualifier, DELIMITER, Bytes.add(value, DELIMITER))
            val scan = Scan(key, Bytes.add(key, ByteArray(0)))
            scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier)
            val indexScanner = indexHTable.getScanner(scan)

            val gets = ArrayList<Get>()
            for (result in indexScanner) {
                for (cell in result.listCells()) {
                    gets.add(Get(CellUtil.cloneValue(cell)))
                }
            }
            return sourceHTable.get(gets)
        } catch (e: Exception) {
            throw IOException("Could not rollback transaction", e)
        }
    }

    @Throws(IOException::class)
    fun put(put: Put) {
        put(listOf(put))
    }

    /**
     * 写入的记录的时候，根据操作的对象是否包含二次索引，进行写入操作
     */
    @Throws(IOException::class)
    fun put(puts: List<Put>) {
        try {
            val secondaryIndexPuts = ArrayList<Put>()
            for (put in puts) {
                val indexPuts = ArrayList<Put>()
                val familyMap = put.familyCellMap.entries
                for ((_, value1) in familyMap) {
                    for (value in value1) {
                        if (Bytes.equals(value.qualifierArray, value.qualifierOffset, value.qualifierLength,
                                        secondaryIndexColumn, 0, secondaryIndexColumn.size)) {
                            // 二次索引的rowkey为：索引名|索引值|Rowkey
                            val secondaryRow = Bytes.add(
                                    CellUtil.cloneQualifier(value),
                                    DELIMITER,
                                    Bytes.add(CellUtil.cloneValue(value), DELIMITER, CellUtil.cloneRow(value)))
                            val indexPut = Put(secondaryRow)
                            indexPut.addColumn(secondaryIndexFamily, secondaryIndexQualifier, put.row)
                            indexPuts.add(indexPut)
                        }
                    }
                }
                secondaryIndexPuts.addAll(indexPuts)
            }
            sourceHTable.put(puts)
            indexHTable.put(secondaryIndexPuts)
        } catch (e: Exception) {
            throw IOException("Could not write secondary index", e)
        }
    }

    @Throws(IOException::class)
    override fun close() {
        try {
            indexHTable.close()
        } catch (e: IOException) {
            throw e
        }
    }
}
