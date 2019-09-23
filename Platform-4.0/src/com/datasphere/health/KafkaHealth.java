package com.datasphere.health;

public class KafkaHealth implements ComponentHealth
{
    public long latestActivity;
    public String fqKafkaName;
    
    public KafkaHealth(final String fqCacheName) {
        this.fqKafkaName = fqCacheName;
    }
    
    public KafkaHealth(final long latestActivity, final String fqKafkaName) {
        this.latestActivity = latestActivity;
        this.fqKafkaName = fqKafkaName;
    }
    
    public void update(final long latestActivity, final String fqKafkaName) {
        this.latestActivity = latestActivity;
        this.fqKafkaName = fqKafkaName;
    }
    
    public String getFqKafkaName() {
        return this.fqKafkaName;
    }
    
    public long getLatestActivity() {
        return this.latestActivity;
    }
}
