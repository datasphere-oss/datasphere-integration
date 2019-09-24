package com.datasphere.kafka;
/*
 * Kafka 节点信息
 */
public class KafkaNode
{
    public final int id;
    public final String host;
    public final int port;
    
    public KafkaNode(final int id, final String host, final int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }
    
    @Override
    public boolean equals(final Object obj) {
        return obj instanceof KafkaNode && this.host.equals(((KafkaNode)obj).host) && this.port == ((KafkaNode)obj).port && this.id == ((KafkaNode)obj).id;
    }
    
    @Override
    public int hashCode() {
        return this.id;
    }
    
    public String getIpPort() {
        return this.host.concat(":").concat(String.valueOf(this.port));
    }
    
    @Override
    public String toString() {
        return "KafkaNode(id:" + this.id + ", host:" + this.host + ", port:" + this.port + ")";
    }
}
