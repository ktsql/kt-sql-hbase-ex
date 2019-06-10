package me.principality.ktsql.backend.hbase.index.lucene

import me.principality.ktsql.backend.hbase.utils.ConfigProvider
import org.apache.lucene.store.Directory
import org.apache.lucene.store.Lock
import org.apache.lucene.store.LockFactory
import org.apache.lucene.store.LockObtainFailedException
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import java.io.IOException
import java.util.*

/**
 * 使用Redis实现分布式锁
 */
class RedisLockFactory : LockFactory() {
    val jedis = Jedis(ConfigProvider.redis)
    private val LOCK_SUCCESS = "OK"

    @Throws(IOException::class)
    override fun obtainLock(dir: Directory, lockKey: String): Lock {
        val expireTime = 0L
        val requestId = UUID.randomUUID().toString()

        val params = SetParams()
        params.nx()
        params.px(expireTime)

        try {
            val result = jedis.set(lockKey, requestId, params)

            return if (LOCK_SUCCESS.equals(result)) {
                RedisLock(lockKey, requestId, jedis)
            } else {
                throw LockObtainFailedException("can not obtain redis lock")
            }
        } catch (e: Exception) {
            throw IOException(e)
        }
    }
}
