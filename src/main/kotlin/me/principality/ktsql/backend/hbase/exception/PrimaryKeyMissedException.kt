package me.principality.ktsql.backend.hbase.exception

import org.apache.calcite.runtime.CalciteException

class PrimaryKeyMissedException(message: String,
                                cause: Throwable) : CalciteException(message, cause) {
}