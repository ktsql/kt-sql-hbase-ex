# 概述

该项目用于实现calcite对HBase的支持，在该项目的支持下，calcite获得以下功能：
1. calcite-hbase-adaptor
2. calcite-server ddl扩展所需的底层存储功能支持
   - 表创建/删除/查询
   - 索引创建/删除/查询
3. 设计并实现SQL元数据管理所需的HBase处理功能

## 元数据管理

### 元数据的实现思路

通过表命名和系统表的组合方式，实现元数据

### 表命名

表命名的格式为：{表名}.{系统功能}.{扩展描述}

定义系统功能关键字如下：

1. sys 表示系统表
2. kvidx 表示KV型索引表

一个实现了KV型二次索引的表table，会生成一个table.kvindex.index_name的表，
每一行的数据，都保存了从索引键到rowkey的对应关系

### 系统表

待定，看SqlSchema的实际需求