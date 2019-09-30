### MSSQL数据集成介绍
____________________________

#### MSSQL同步到MySQL
____________________________

* 配置数据库源端

1. 选择MSSQL数据库作为源端数据库
2. 为数据源命名: MSSQLSource
3. 选择处理器: MSSQLReader
4. 填写源端数据库的用户名: myuser
5. 填写连接数据库的 JDBC 驱动 URL: 192.168.0.1:1433
6. 填写数据库需要同步的表: dbo.mytable
7. 填写源端数据库的密码:*******

* 配置数据库目标端

1. 选择输入流为:MSSQLStream
2. 选择处理器: DatabaseWriter
3. 为数据目标命名: HashdataTarget
4. 填写目标数据库的用户名: root
5. 填写连接目标数据库的 JDBC 驱动 URL: 192.168.0.2:3306/mydb
6. 填写目标数据库的表清单: dbo.mytable,ROOT.TEST;
7. 填写目标数据库的密码:*******
8. 根据你的需要设置同步参数：

> 批量策略: EventCount:1,Interval:0


#### MSSQL同步到PostgreSQL
____________________________

* 配置数据库源端

1. 选择MSSQL数据库作为源端数据库
2. 为数据源命名: MSSQLSource
3. 选择处理器: MSSQLReader
4. 填写源端数据库的用户名: myuser
5. 填写连接数据库的 JDBC 驱动 URL: 192.168.0.1:1433
6. 填写数据库需要同步的表: dbo.mytable
7. 填写源端数据库的密码:*******


* 配置数据库目标端

1. 选择输入流为:MSSQLSource
2. 选择处理器: DatabaseWriter
3. 为数据目标命名: PostgreSQLTarget
4. 填写目标数据库的用户名: root
5. 填写连接目标数据库的 JDBC 驱动 URL: jdbc:postgresql://192.168.0.2:5432/postgres
6. 填写目标数据库的表清单: dbo.mytable,POSTGRES.TEST;
7. 填写目标数据库的密码:*******
8. 根据你的需要设置同步参数：

> 批量策略: EventCount:1,Interval:0





#### MSSQL同步到Hashdata
____________________________

* 配置数据库源端

1. 选择MSSQL数据库作为源端数据库
2. 为数据源命名: MSSQLSource
3. 选择处理器: MSSQLReader
4. 填写源端数据库的用户名: myuser
5. 填写连接数据库的 JDBC 驱动 URL: 192.168.0.1:1433
6. 填写数据库需要同步的表: dbo.mytable
7. 填写源端数据库的密码:*******


* 配置数据库目标端

1. 选择输入流为:MSSQLSource
2. 选择处理器: DatabaseWriter
3. 为数据目标命名: HashdataTarget
4. 填写目标数据库的用户名: gpadmin
5. 填写连接目标数据库的 JDBC 驱动 URL: jdbc:postgresql://192.168.0.2:5432/gpadmin
6. 填写目标数据库的表清单: dbo.mytable,GPADMIN.TEST;
7. 填写目标数据库的密码:*******
8. 根据你的需要设置同步参数：

> 批量策略: EventCount:1,Interval:0


#### MSSQL同步到Hive
____________________________

* 配置数据库源端

1. 选择MSSQL数据库作为源端数据库
2. 为数据源命名: MSSQLSource
3. 选择处理器: MSSQLReader
4. 填写源端数据库的用户名: myuser
5. 填写连接数据库的 JDBC 驱动 URL: 192.168.0.1:1433
6. 填写数据库需要同步的表: dbo.mytable
7. 填写源端数据库的密码:*******


* 配置数据库目标端

1. 选择输入流为:MSSQLSource
2. 选择处理器: HiveWriter
3. 为数据目标命名: HiveTarget
4. 填写目标数据库的用户名: 
5. 填写连接目标数据库的 JDBC 驱动 URL: jdbc:hive2://192.168.0.2:10000/hive
6. 填写目标数据库的表清单: dbo.mytable,HIVE.TEST;
7. 填写目标数据库的密码:*******
8. 根据你的需要设置同步参数：

> 批量策略: EventCount:1,Interval:0



