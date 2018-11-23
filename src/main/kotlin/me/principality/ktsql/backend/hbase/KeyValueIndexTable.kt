package me.principality.ktsql.backend.hbase

import com.google.common.base.Throwables
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
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
    private val connection: Connection
    private val secondaryIndex: ByteArray

    @Throws(IOException::class)
    constructor(table: Table,
                secondaryIndex: ByteArray,
                indexName: String,
                indexType: String = HBaseTable.IndexType.KEY_VALUE.name) {
        secondaryIndexTableName = TableName.valueOf(HBaseUtils.indexTableName(table.name.nameAsString, indexName, indexType))
        this.connection = HBaseConnection.connection()
        this.secondaryIndex = secondaryIndex
        var secondaryIndexHTable: Table? = null
        try {
            this.connection.admin.use { hBaseAdmin ->
                if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
                    val tableDescriptor = HTableDescriptor(secondaryIndexTableName)
                    tableDescriptor.addFamily(HColumnDescriptor(HBaseTable.columnFamily))
                    hBaseAdmin.createTable(tableDescriptor)
//                    hBaseAdmin.createTable(TableDescriptorBuilder.newBuilder(secondaryIndexTableName).build())
                }
                secondaryIndexHTable = this.connection.getTable(secondaryIndexTableName)
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

    @Throws(IOException::class)
    operator fun get(get: Get): Result {
        return get(listOf(get))!![0]
    }

    @Throws(IOException::class)
    operator fun get(gets: List<Get>): Array<Result>? {
        try {
            val result = sourceHTable.get(gets)
            return result
        } catch (e: Exception) {
            throw IOException("Could not rollback transaction", e)
        }
    }

    @Throws(IOException::class)
    fun getByIndex(value: ByteArray): Array<Result>? {
        try {
            val scan = Scan(value, Bytes.add(value, ByteArray(0)))
            scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier)
            val indexScanner = indexHTable.getScanner(scan)

            val gets = ArrayList<Get>()
            for (result in indexScanner) {
                for (cell in result.listCells()) {
                    gets.add(Get(CellUtil.cloneValue(cell)))
                }
            }
            val results = sourceHTable.get(gets)
            return results
        } catch (e: Exception) {
            throw IOException("Could not rollback transaction", e)
        }
    }

    @Throws(IOException::class)
    fun put(put: Put) {
        put(listOf(put))
    }

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
                                        secondaryIndex, 0, secondaryIndex.size)) {
                            val secondaryRow = Bytes.add(CellUtil.cloneQualifier(value), DELIMITER,
                                    Bytes.add(CellUtil.cloneValue(value), DELIMITER,
                                            CellUtil.cloneRow(value)))
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
            throw IOException("Could not rollback transaction", e)
        }
    }

    @Throws(IOException::class)
    override fun close() {
        try {
            sourceHTable.close()
        } catch (e: IOException) {
            try {
                indexHTable.close()
            } catch (ex: IOException) {
                e.addSuppressed(ex)
            }

            throw e
        }

        indexHTable.close()
    }
}
