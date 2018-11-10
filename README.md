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
2. kv 表示KV型索引表

一个实现了KV型二次索引的表table，会生成一个table.kv.index_name的表，
每一行的数据，都保存了从索引键到rowkey的对应关系

### 系统表

待定，看SqlSchema的实际需求

初步考虑，把表放在table.sys，字段放在column.sys表中
table.sys的rowkey设计为：schema_path+name，包含字段：
1. 是否为事务表
2. 索引类型
3. 表锁定状态，DDL修改时会锁定 https://ktsql.github.io/2018/11/03/分布式数据库的DDL实现/
4. 创建时间
5. 字符集
6. 备注

column.sys的rowkey设计为：schema_path+tablename+column_name，包含字段：
1. 默认值
2. 字段是否为空
3. 数据类型
4. 最大长度 <- 依赖javaType
5. 精度 <- 同上
6. 排序
7. 备注

主键必须在创建表时指定，否则会报错。支持多个字段组成主键。主键创建对性能影响很大，需要谨慎选择

表字段是否唯一、约束暂不考虑，待逐步完善