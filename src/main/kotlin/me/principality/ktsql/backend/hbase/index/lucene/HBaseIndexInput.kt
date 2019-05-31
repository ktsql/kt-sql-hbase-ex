package me.principality.ktsql.backend.hbase.index.lucene

import org.apache.lucene.store.IndexInput

/**
 * 实现HBaseIndexInput，底层的存储使用HBase
 *
 * 如果是很大的数据量（单个文件，则考虑采用分割到多行的做法？）
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