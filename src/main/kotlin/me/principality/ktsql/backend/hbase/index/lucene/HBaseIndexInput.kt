package me.principality.ktsql.backend.hbase.index.lucene

import me.principality.ktsql.backend.hbase.HBaseConnection
import me.principality.ktsql.backend.hbase.HBaseTable
import me.principality.ktsql.backend.hbase.HBaseUtils
import me.principality.ktsql.backend.hbase.index.lucene.LuceneIndexConstants.CONTENT
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.store.IndexInput

/**
 * 实现HBaseIndexInput，底层的存储使用HBase
 *
 * 如果是很大的数据量（单个文件，则考虑采用分割到多行的做法？）
 */
class HBaseIndexInput(resourceDescription: String?) : IndexInput(resourceDescription) {
    private var position = 0L
    private val table: Table
    private val indexValue: ByteArray

    init {
        table = HBaseConnection.connection().getTable(TableName.valueOf(HBaseUtils.LUCENE_INDEX_TABLE_NAME))
        val get = Get(Bytes.toBytes(resourceDescription))
        val dbresult = table.get(get)
        indexValue = dbresult.getValue(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(CONTENT))
    }

    override fun length(): Long {
        return indexValue.size.toLong()
    }

    override fun seek(pos: Long) {
        position = pos
    }

    override fun readByte(): Byte {
        return indexValue.get(position.toInt())
    }

    /**
     * 重新创建一个分片，相当于在HBase里面新插入一行，并以此为基础，返回IndexInput
     */
    override fun slice(sliceDescription: String?, offset: Long, length: Long): IndexInput {
        // 插入一行
        val put = Put(Bytes.toBytes(sliceDescription))
        put.addColumn(Bytes.toBytes(HBaseTable.columnFamily), Bytes.toBytes(CONTENT),
                indexValue.slice(offset.toInt()..(offset + length).toInt()).toByteArray())
        table.put(put)
        // 返回
        return HBaseIndexInput(sliceDescription)
    }

    override fun getFilePointer(): Long {
        return position
    }

    override fun close() {
        table.close()
    }

    /**
     * 按字符逐字拷贝
     */
    override fun readBytes(b: ByteArray?, offset: Int, len: Int) {
        for (i in 0..len) {
            b?.set(i, indexValue.get(offset + i))
        }
    }
}