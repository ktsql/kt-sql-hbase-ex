package me.principality.ktsql.utils.collections

import com.google.common.collect.ImmutableSortedMap
import me.principality.ktsql.backend.hbase.HBaseConnection
import org.apache.calcite.linq4j.function.Experimental
import org.apache.calcite.util.NameMap
import org.apache.calcite.util.NameSet.COMPARATOR
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes
import java.util.*

/**
 * 实现class NameMap<V>的接口，并作为其替代
 */
class RemoteNameMap<V> : NameMap<V> {
    private val table: Table
    private val family: String = "columnFamily"

    constructor(name: String) {
        table = HBaseConnection.connection().getTable(TableName.valueOf(name))
    }

    companion object {
        /** Creates a NameMap that is an immutable copy of a given map.  */
        fun <V> immutableCopyOf(names: Map<String, V>): NameMap<*> {
            return NameMap.immutableCopyOf(names)
        }
    }

    override fun toString(): String {
        return table.toString()
    }

    override fun hashCode(): Int {
        return table.hashCode()
    }

    override fun equals(obj: Any?): Boolean {
        return this === obj || obj is RemoteNameMap<*> && table == obj.table
    }

    override fun put(key: String, v: V) {
        //进行数据插入
        val put = Put(Bytes.toBytes(generateRowkey(key)))
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(v as String))
        table.put(put)
    }

    /** Returns a map containing all the entries in the map that match the given
     * name. If case-sensitive, that map will have 0 or 1 elements; if
     * case-insensitive, it may have 0 or more.  */
    override fun range(name: String, caseSensitive: Boolean): NavigableMap<String, V> {
        val map = TreeMap<String, String>(COMPARATOR)
        val scan = Scan()
        var rs: ResultScanner? = null
        try {
            rs = table.getScanner(scan)

            for ((k, v) in rs!!.withIndex()) {
                map.put(v.getRow().toString(), v.value().toString())
            }
        } catch (e: Exception) {
            rs!!.close()
        }

        if (caseSensitive) {
            if (map.containsKey(name)) {
                return ImmutableSortedMap.of(name, map.get(name)) as NavigableMap<String, V>
            } else {
                return ImmutableSortedMap.of()
            }
        } else {
            return map.subMap(name.toUpperCase(Locale.ROOT), true,
                    name.toLowerCase(Locale.ROOT), true) as NavigableMap<String, V>
        }
    }

    /** Returns whether this map contains a given key, with a given
     * case-sensitivity.  */
    override fun containsKey(name: String, caseSensitive: Boolean): Boolean {
        return !range(name, caseSensitive).isEmpty()
    }

    /** Returns the underlying map.  */
    override fun map(): NavigableMap<String, V> {
        val map = TreeMap<String, String>(COMPARATOR)
        val scan = Scan()
        var rs: ResultScanner? = null
        try {
            rs = table.getScanner(scan)

            for ((k, v) in rs!!.withIndex()) {
                map.put(v.getRow().toString(), v.value().toString())
            }
        } catch (e: Exception) {
            rs!!.close()
        }
        return map as NavigableMap<String, V>
    }

    fun get(key: String): Optional<V> {
        val range = range(key, false)
        if (!range.isEmpty()) {
            return Optional.of(range.get(key)!!)
        }
        return Optional.empty()
    }

    /**
     * 因为kotlin语言限制的问题，此处采用V?作为返回值
     */
    @Experimental
    override fun remove(key: String): V? {
        val range = range(key, false)
        if (!range.isEmpty()) {
            val rowKey = generateRowkey(key)
            val delete = Delete(Bytes.toBytes(rowKey))
            table.delete(delete)
            return range.get(key)!!
        }
        return null
    }

    private fun generateRowkey(key: String): String {
        return table.name.toString() + key
    }
}