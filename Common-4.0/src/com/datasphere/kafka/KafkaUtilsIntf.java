package com.datasphere.kafka;

import java.util.*;
/*
 * Kafka 工具类接口
 * 创建主题
 * 删除主题
 * 获得中继器分区
 * 从主题中获得最新记录
 * 查询消费者
 * 获得生产者
 * 获得消费者
 * 验证配置文件属性
 */
public interface KafkaUtilsIntf
{
    void createTopic(final String p0, final String p1, final int p2, final int p3, final Properties p4, final long p5) throws Exception;
    
    void deleteTopic(final String p0, final String p1, final long p2) throws Exception;
    
    Map<KafkaNode, List<Integer>> getBrokerPartitions(final List<String> p0, final String p1, final Map<String, String> p2, final String p3) throws KafkaException;
    
    KafkaMessageAndOffset getLastRecordFromTopic(final String p0, final int p1, final KafkaNode p2, final String p3, final Map<String, String> p4) throws KafkaException;
    
    List<KafkaMessageAndOffset> fetch(final ConsumerIntf p0, final long p1, final List<String> p2, final String p3, final int p4, final KafkaNode p5, final String p6, final Map<String, String> p7) throws KafkaException;
    
    ProducerIntf getKafkaProducer(final Properties p0);
    
    ConsumerIntf getKafkaConsumer(final Properties p0) throws KafkaException;
    
    void validateProperties(final Map<String, String> p0, final Map<String, String> p1, final Map<String, String> p2) throws KafkaException;
}
