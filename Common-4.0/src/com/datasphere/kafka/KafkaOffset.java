package com.datasphere.kafka;
/*
 * Kafka 偏移量信息
 */
public class KafkaOffset
{
    public final long[] offset;
    
    public KafkaOffset(final long[] offset) {
        this.offset = offset;
    }
}
