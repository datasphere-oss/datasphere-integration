package com.datasphere.kafka;

import com.datasphere.recovery.*;
/*
 * 分区状态信息
 * 主要保存Kafka和检查点的分区信息
 */
public class PartitionState
{
    public final int partitionId;
    public long kafkaReadOffset;
    public KafkaNode dataTopicBroker;
    public PathManager partitionCheckpoint;
    public long checkpointOffset;
    public long checkpointTimestamp;
    public KafkaNode checkpointTopicBroker;
    public long lastCheckpointWriteOffset;
    public Position lastEmittedPosition;
    
    public PartitionState(final int partitionId) {
        this.kafkaReadOffset = 0L;
        this.dataTopicBroker = null;
        this.partitionCheckpoint = null;
        this.checkpointOffset = -1L;
        this.checkpointTimestamp = -1L;
        this.checkpointTopicBroker = null;
        this.lastCheckpointWriteOffset = -1L;
        this.partitionId = partitionId;
    }
    
    @Override
    public String toString() {
        return "PartitionState{" + this.partitionId + "@" + this.kafkaReadOffset + " bro=" + this.dataTopicBroker + " part=" + this.partitionCheckpoint + " chBro=" + this.checkpointTopicBroker + " lastWrite=" + this.lastCheckpointWriteOffset + "}";
    }
}
