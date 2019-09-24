package com.datasphere.kafka;

import java.util.*;
/*
 * 消费者接口
 */
public interface ConsumerIntf
{
    List<KafkaMessageAndOffset> read(final PartitionState p0) throws KafkaException;
    
    void close();
    
    void wakeup();
    
    String getId();
}
