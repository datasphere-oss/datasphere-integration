package com.datasphere.kafka;

import java.util.*;
/*
 * 设置 Kafka 消息信息和偏移量
 */
public class KafkaMessageAndOffset
{
    public byte[] data;
    public final long offset;
    public final long nextOffset;
    
    public KafkaMessageAndOffset(final byte[] data, final long offset, final long nextOffset) {
        this.data = data;
        this.offset = offset;
        this.nextOffset = nextOffset;
    }
    
    @Override
    public String toString() {
        return "DAMessage(offset = " + this.offset + ", data = " + Arrays.toString(this.data) + ", size = " + this.data.length + ")";
    }
}
