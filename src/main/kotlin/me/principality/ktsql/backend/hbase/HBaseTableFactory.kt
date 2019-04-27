package me.principality.ktsql.backend.hbase

/**
 * 如果需要创建自定义类型的表，如在内存中的表、通过配置文件生成的表等，
 * 可以通过TableFactory来创建，在系统元数据管理等场景下，自定义表是很好的选择
 *
 * 参考：https://calcite.apache.org/docs/tutorial.html#custom-tables
 */
@Deprecated("")
class HBaseTableFactory {
}