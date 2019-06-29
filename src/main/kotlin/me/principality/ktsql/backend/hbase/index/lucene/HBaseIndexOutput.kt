package me.principality.ktsql.backend.hbase.index.lucene

import me.principality.ktsql.backend.hbase.HBaseConnection
import me.principality.ktsql.backend.hbase.HBaseTable
import me.principality.ktsql.backend.hbase.HBaseUtils
import me.principality.ktsql.backend.hbase.index.lucene.LuceneIndexConstants.CONTENT
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.store.IndexOutput
import java.util.zip.CRC32

/**
 * 基于HBase的IndexOutput，把HBase的一个表（或者一行）映射为IndexOutput
 */
class HBaseIndexOutput(resourceDescription: String?, name: String?) : IndexOutput(resourceDescription, name) {
    private var position = 0L
    private val table: Table
    private val indexValue: ByteArray

    init {
        table = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.LUCENE_INDEX_TABLE_NAME))
        val get = Get(Bytes.toBytes(resourceDescription))
        val dbresult = table.get(get)
        indexValue = dbresult.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(CONTENT))
    }

    override fun writeBytes(b: ByteArray?, offset: Int, length: Int) {
        for (i in 0..length) {
            indexValue.set(position.toInt() + i, b?.get(offset + i)!!)
        }
        position += length
    }

    override fun writeByte(b: Byte) {
        indexValue.set(position.toInt(), b)
    }

    override fun getFilePointer(): Long {
        return position
    }

    override fun getChecksum(): Long {
        var crc32 = CRC32()

        crc32.update(indexValue)
        return crc32.value
    }

    override fun close() {
        table.close()
    }
}