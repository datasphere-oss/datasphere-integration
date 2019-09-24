package com.datasphere.kafka;

import java.util.*;
/*
 * Kafka 集群相关参数
 * TODO 将 Kafka 集群相关参数迁移至配置中心(ConfigInfoCenter)
 */
public class KafkaConstants
{
    public static final String PARTITIONS_I_OWN = "partitions_i_own";
    public static final String default_kafka_propertyset_name = "DefaultKafkaProperties";
    public static final String dataformat = "dataformat";
    public static final String CHECKPOINT_SUFFIX = "_CHECKPOINT";
    public static final String zk_address_prop_set = "zk.address";
    public static final String default_zk_address = "localhost:2181";
    public static final String boostrap_brokers_prop_set = "bootstrap.brokers";
    public static final String default_brokers = "localhost:9092";
    public static final String jmx_broker_prop_set = "jmx.broker";
    public static final String default_jmx = "localhost:9998";
    public static final String number_of_replicas_property = "replication.factor";
    public static final String default_number_of_replicas = "1";
    public static final String partitions_property = "partitions";
    public static final int default_number_of_partition = 200;
    public static final String fetch_size_property = "com.datasphere.kafka.topic.fetchsize";
    public static final int fetch_size;
    public static final String batch_size_property = "batch.size";
    public static final int default_batch_size = 1572864;
    public static final String linger_property = "linger.ms";
    public static final int default_linger = 200;
    public static final String acks = "com.datasphere.config.kafka.producer.acks";
    public static final String producer_acks;
    public static final String buffer_memory = "com.datasphere.kafka.producer.buffer.memory";
    public static final String PRODUCER_CONFIG = "producerconfig";
    public static final String bootstrap_servers_property = "bootstrap.servers";
    public static final String acks_property = "acks";
    public static final String key_serializer_property = "key.serializer";
    public static final String value_serializer_property = "value.serializer";
    public static final String buffer_memory_property = "buffer.memory";
    public static final String max_request_size_property = "max.request.size";
    public static final String max_send_retries_property = "retries";
    public static final int producer_buffer_memory;
    public static final int producer_batchsize = 0;
    public static final String max_request_size = "com.datasphere.kafka.producer.max.request.size";
    public static final int producer_max_request_size;
    public static final int producer_max_send_retries = 0;
    public static final int linger = 0;
    public static final String string_serializer = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String int_serializer = "org.apache.kafka.common.serialization.IntegerSerializer";
    public static final String bytearray_serializer = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String reconnect_backoff_property = "reconnect.backoff.ms";
    public static final int reconnect_backoff_ms = 10000;
    public static final String retry_backoff_property = "retry.backoff.ms";
    public static final int retry_backoff_ms = 10000;
    public static final String send_buffer_size_property = "send_buffer_size";
    public static final int send_buffer_size;
    static final String broker_address_list_property = "com.datasphere.config.kafka.brokers";
    public static final String flush_period_property = "com.datasphere.config.kafka.flush.interval";
    public static final int flush_period;
    public static final String flush_threads_property = "com.datasphere.config.kafka.flush.threads";
    public static final int flush_threads;
    public static List<String> broker_address_list;
    public static final String total_event_count = "total_event_count";
    public static final String total_bytes_count = "total_bytes_count";
    public static final String total_wait_count = "total_wait_count";
    public static final String events_per_sec = "events_per_sec";
    public static final String megs_per_sec = "megs_per_sec";
    public static final String avg_latency = "avg_latency";
    public static final String max_latency = "max_latency";
    public static final String CONSUMER_CONFIG = "consumerconfig";
    public static final String SECURITY_CONFIG = "securityconfig";
    public static final int init_retry_backoff_ms = 1000;
    public static final int init_retry_count = 3;
    
    static {
        fetch_size = Integer.parseInt(System.getProperty("com.datasphere.kafka.topic.fetchsize", Integer.toString(43264200)));
        producer_acks = System.getProperty("com.datasphere.config.kafka.producer.acks", "all");
        producer_buffer_memory = Integer.parseInt(System.getProperty("com.datasphere.kafka.producer.buffer.memory", Integer.toString(67108864)));
        producer_max_request_size = Integer.parseInt(System.getProperty("com.datasphere.kafka.producer.max.request.size", Integer.toString(43264200)));
        send_buffer_size = Integer.parseInt(System.getProperty("send_buffer_size", Integer.toString(1500000)));
        flush_period = Integer.parseInt(System.getProperty("com.datasphere.config.kafka.flush.interval", Integer.toString(200)));
        flush_threads = Integer.parseInt(System.getProperty("com.datasphere.config.kafka.flush.threads", Integer.toString(1)));
    }
}
