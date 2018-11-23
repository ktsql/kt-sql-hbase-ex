package me.principality.ktsql.backend.hbase

import com.google.common.base.Throwables
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes
import org.apache.tephra.TransactionContext
import org.apache.tephra.TransactionFailureException
import org.apache.tephra.distributed.TransactionServiceClient
import org.apache.tephra.hbase.TransactionAwareHTable
import java.io.Closeable
import java.io.IOException
import java.util.*

/**
 * 二次索引实现，基于tephra保证索引和数据变更的强一致性，待依据设计进一步改进
 */
class KeyValueIndexTransTable : Closeable {
    private val transactionAwareHTable: TransactionAwareHTable
    private val secondaryIndexTable: TransactionAwareHTable
    private val transactionContext: TransactionContext
    private val secondaryIndexTableName: TableName
    private val connection: Connection
    private val secondaryIndex: ByteArray

    @Throws(IOException::class)
    constructor(transactionServiceClient: TransactionServiceClient,
                table: Table,
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
//                    hBaseAdmin.createTable(TableDescriptorBuilder.newBuilder(secondaryIndexTableName).build())
                }
                secondaryIndexHTable = this.connection.getTable(secondaryIndexTableName)
            }
        } catch (e: Exception) {
            Throwables.propagate(e)
        }

        this.transactionAwareHTable = TransactionAwareHTable(table)
        this.secondaryIndexTable = TransactionAwareHTable(secondaryIndexHTable!!)
        this.transactionContext = TransactionContext(transactionServiceClient, transactionAwareHTable,
                secondaryIndexTable)
    }

    companion object {
        private val secondaryIndexFamily = Bytes.toBytes("secondaryIndexFamily")
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
            transactionContext.start()
            val result = transactionAwareHTable.get(gets)
            transactionContext.finish()
            return result
        } catch (e: Exception) {
            try {
                transactionContext.abort()
            } catch (e1: TransactionFailureException) {
                throw IOException("Could not rollback transaction", e1)
            }
        }

        return null
    }

    @Throws(IOException::class)
    fun getByIndex(value: ByteArray): Array<Result>? {
        try {
            transactionContext.start()
            val scan = Scan(value, Bytes.add(value, ByteArray(0)))
            scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier)
            val indexScanner = secondaryIndexTable.getScanner(scan)

            val gets = ArrayList<Get>()
            for (result in indexScanner) {
                for (cell in result.listCells()) {
                    gets.add(Get(CellUtil.cloneValue(cell)))
                }
            }
            val results = transactionAwareHTable.get(gets)
            transactionContext.finish()
            return results
        } catch (e: Exception) {
            try {
                transactionContext.abort()
            } catch (e1: TransactionFailureException) {
                throw IOException("Could not rollback transaction", e1)
            }
        }

        return null
    }

    @Throws(IOException::class)
    fun put(put: Put) {
        put(listOf(put))
    }

    @Throws(IOException::class)
    fun put(puts: List<Put>) {
        try {
            transactionContext.start()
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
            transactionAwareHTable.put(puts)
            secondaryIndexTable.put(secondaryIndexPuts)
            transactionContext.finish()
        } catch (e: Exception) {
            try {
                transactionContext.abort()
            } catch (e1: TransactionFailureException) {
                throw IOException("Could not rollback transaction", e1)
            }
        }
    }

    @Throws(IOException::class)
    override fun close() {
        try {
            transactionAwareHTable.close()
        } catch (e: IOException) {
            try {
                secondaryIndexTable.close()
            } catch (ex: IOException) {
                e.addSuppressed(ex)
            }

            throw e
        }

        secondaryIndexTable.close()
    }
}
