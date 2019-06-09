package me.principality.ktsql.backend.hbase.utils

import com.typesafe.config.ConfigFactory

object ConfigProvider {
    val flavor: String
    val zkquorum: String
    val redis: String

    private val config = ConfigFactory.load()

    init {
        flavor = config.getString("flavor")
        zkquorum = config.getString("zkquorum")
        redis = config.getString("redis")
    }
}