package me.principality.ktsql.backend.hbase.index.lucene

import org.apache.lucene.store.Lock
import org.apache.lucene.store.LockReleaseFailedException
import redis.clients.jedis.Jedis
import java.io.IOException
import java.util.Collections.singletonList

class RedisLock(lockKey: String, requestId: String, redis: Jedis) : Lock() {
    private val lock = lockKey
    private val uuid = requestId
    private val jedis = redis
    private val RELEASE_SUCCESS = 1L

    /**
     * 解锁已经设置的lock
     */
    override fun close() {
        try {
            val requestId: String = uuid
            val script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
            val result = jedis.eval(script, singletonList(lock), singletonList(requestId))

            if (!RELEASE_SUCCESS.equals(result)) {
                throw LockReleaseFailedException("can not release redis lock")
            }
        } catch (e: Exception) {
            throw IOException(e)
        }
    }

    /**
     * 检查lock是否还存在
     */
    override fun ensureValid() {
        if (!jedis.exists(lock)) {
            throw IOException("redis ${lock} not exists")
        }
    }
}