package me.principality.ktsql.backend.hbase.exception

import org.apache.calcite.runtime.CalciteException

class IndexExistsException(message: String) : CalciteException(message, Exception(message)) {
}