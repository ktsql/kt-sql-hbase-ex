package me.principality.ktsql.backend.hbase

import org.apache.calcite.linq4j.Enumerator

/**
 * 使用HBase ResultSet实现
 */
class HBaseEnumerator<E> : Enumerator<E> {
    override fun moveNext(): Boolean {
        TODO("not implemented")
    }

    override fun current(): E {
        TODO("not implemented")
    }

    override fun reset() {
        TODO("not implemented")
    }

    override fun close() {
        TODO("not implemented")
    }
}