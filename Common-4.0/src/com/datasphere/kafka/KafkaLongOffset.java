package com.datasphere.kafka;
/*
 * 设置 Kafka 偏移量
 */
public class KafkaLongOffset extends Offset<Long>
{
    private long offset;
    
    public KafkaLongOffset(final long offset) {
        this.offset = offset;
    }
    
    @Override
    public int compareTo(final Offset o) {
        final long otherValue = ((KafkaLongOffset)o).offset;
        final long diff = this.offset - otherValue;
        if (diff < 0L) {
            return -1;
        }
        if (diff > 0L) {
            return 1;
        }
        return 0;
    }
    
    @Override
    public boolean isValid() {
        return this.offset >= 0L;
    }
    
    @Override
    public Long getOffset() {
        return this.offset;
    }
    
    @Override
    public void setOffset(final Long value) {
        this.offset = value;
    }
    
    @Override
    public String getName() {
        return "Kafka Message offset-" + this.offset;
    }
    
    @Override
    public String toString() {
        return String.valueOf(this.offset);
    }
}
