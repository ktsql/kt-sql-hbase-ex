package me.principality.ktsql.backend.hbase.exception

import org.apache.calcite.runtime.CalciteException

class IllegalColumnNameException(message: String) : CalciteException(message, Exception(message)) {
}