package com.datasphere.recovery;

import org.apache.log4j.*;

public class KafkaSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -6042045728463138984L;
    private Long kafkaPosition;
    private Long recordSeqNo;
    private String currentPartitionID;
    private int partitionID;
    private String topic;
    private static final Logger logger;
    
    public KafkaSourcePosition() {
        this.kafkaPosition = 0L;
        this.recordSeqNo = 0L;
        this.currentPartitionID = null;
    }
    
    public KafkaSourcePosition(final KafkaSourcePosition sp) {
        this.kafkaPosition = 0L;
        this.recordSeqNo = 0L;
        this.kafkaPosition = sp.kafkaPosition;
        this.recordSeqNo = sp.recordSeqNo;
        this.currentPartitionID = sp.currentPartitionID;
        this.topic = sp.topic;
        this.partitionID = sp.partitionID;
    }
    
    public KafkaSourcePosition(final String topic, final int partitionID, final long recordSeqNo, final long kafkaReadOffset) {
        this.kafkaPosition = 0L;
        this.recordSeqNo = 0L;
        this.topic = topic;
        this.partitionID = partitionID;
        this.currentPartitionID = topic + ":" + partitionID;
        this.recordSeqNo = recordSeqNo;
        this.kafkaPosition = kafkaReadOffset;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final KafkaSourcePosition that = (KafkaSourcePosition)arg0;
        if (!this.currentPartitionID.equals(that.currentPartitionID)) {
            throw new RuntimeException("This and that positions were from different paths. This - " + this.toString() + " That - " + that.toString());
        }
        final int compareKafkaReadOffset = this.kafkaPosition.compareTo(that.kafkaPosition);
        if (compareKafkaReadOffset == 0) {
            return this.recordSeqNo.compareTo(that.recordSeqNo);
        }
        return compareKafkaReadOffset;
    }
    
    @Override
    public String toString() {
        final String result = this.currentPartitionID + " Offset=" + this.kafkaPosition + "/" + this.recordSeqNo;
        return result;
    }
    
    public String getCurrentPartitionID() {
        return this.currentPartitionID;
    }
    
    public long getRecordSeqNo() {
        return this.recordSeqNo;
    }
    
    public long getKafkaReadOffset() {
        return this.kafkaPosition;
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaSourcePosition.class);
    }
}
