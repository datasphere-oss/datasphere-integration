package com.datasphere.target.kafka.serializer;

public interface KafkaMessageSerilaizer
{
    byte[] convertToBytes(final Object p0) throws Exception;
}
