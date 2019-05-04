package me.principality.ktsql.backend.hbase.index.lucene

import org.apache.lucene.store.IndexInput

/**
 * 实现HBaseIndexInput
 */
class HBaseIndexInput(resourceDescription: String?) : IndexInput(resourceDescription) {
    override fun length(): Long {
        TODO("not implemented")
    }

    override fun seek(pos: Long) {
        TODO("not implemented")
    }

    override fun readByte(): Byte {
        TODO("not implemented")
    }

    override fun slice(sliceDescription: String?, offset: Long, length: Long): IndexInput {
        TODO("not implemented")
    }

    override fun getFilePointer(): Long {
        TODO("not implemented")
    }

    override fun close() {
        TODO("not implemented")
    }

    override fun readBytes(b: ByteArray?, offset: Int, len: Int) {
        TODO("not implemented")
    }
}