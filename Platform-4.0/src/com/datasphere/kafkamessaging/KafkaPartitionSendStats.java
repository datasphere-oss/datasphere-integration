package com.datasphere.kafkamessaging;

public class KafkaPartitionSendStats
{
    long sentCount;
    long time;
    long bytes;
    long waitCount;
    long maxLatency;
    long iterations;
    
    public KafkaPartitionSendStats() {
        this.resetValues();
    }
    
    public void resetValues() {
        final long n = 0L;
        this.iterations = n;
        this.maxLatency = n;
        this.waitCount = n;
        this.bytes = n;
        this.time = n;
        this.sentCount = n;
    }
    
    public void record(final long time, final long bytes, final long sentCount, final long waitCount) {
        ++this.iterations;
        this.time += time;
        this.maxLatency = Math.max(this.maxLatency, time);
        this.bytes += bytes;
        this.sentCount += sentCount;
        this.waitCount += waitCount;
    }
}
