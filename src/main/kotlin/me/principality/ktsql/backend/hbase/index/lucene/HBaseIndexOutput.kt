package me.principality.ktsql.backend.hbase.index.lucene

import org.apache.lucene.store.IndexOutput

/**
 * 基于HBase的IndexOutput，把HBase的一个表（或者一行）映射为IndexOutput
 */
class HBaseIndexOutput(resourceDescription: String?, name: String?) : IndexOutput(resourceDescription, name)
{
    override fun writeBytes(b: ByteArray?, offset: Int, length: Int) {
        TODO("not implemented")
    }

    override fun writeByte(b: Byte) {
        TODO("not implemented")
    }

    override fun getFilePointer(): Long {
        TODO("not implemented")
    }

    override fun getChecksum(): Long {
        TODO("not implemented")
    }

    override fun close() {
        TODO("not implemented")
    }
}