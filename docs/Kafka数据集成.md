### Kafka数据集成介绍
____________________________

#### MySQL同步到Kafka
____________________________

* 配置 Kafka 队列集群服务

1. 输入命令 cd Kafka/bin，进入到 Integration 服务器的kafka的bin目录。

2. 进入到 bin 目录后可以看到如下的 Kafka 服务启动脚本。


* 启动 Zookeeper 服务

1. sudo ./zookeeper-server-start.sh ../config/zookeeper.properties

2. 执行此命令后，可以看到 Zookeeper 服务正常启动成功。

3. zookeeper 服务启动后，再启动 Kafka Server。


* 启动 Kafka Server服务

1. 在server.properties文件中，设置zookeeper 连接地址

2. sudo ./kafka-server-start.sh ../config/server.properties

3. 执行此命令后，可以看到 Kafka服务正常启动成功


* 启动 Kafka Consumer服务

1. 在config 目录中修改 consumer.properties中的zookeeper 连接地址

2. 进入到在 Integration/Kafka/bin目录下, 输入如下命令
sudo ./kafka-console-consumer.sh --topic mysqlstream  --zookeeper 127.0.0.1:2181 --from-beginning
其中 kafka 的 topic 名称为 mysqlstream 。 


* 配置MySQL 到 Kafka 同步任务

1. 选择适配处理器为 KafkaWriter

2. 设置主题为 mysqltopic（此主题与Kafka消费者主题保持一致）

3. 设置代理器地址为 Kafka 服务器地址127.0.0.1:9092

4. 设置 Kafka 配置为retry.backoff.ms=10000

5. 选择格式化处理器为DSVFormatter


* 启动MySQL到Kafka的数据同步任务

























