package com.datasphere.jmqmessaging;

import java.io.*;
import org.apache.log4j.*;

import com.datasphere.messaging.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
/*
 * Kafka 接收器信息
 */
public class KafkaReceiverInfo extends ReceiverInfo implements Serializable, KryoSerializable
{
    private static final Logger logger;
    private static final long serialVersionUID = 88218429128226711L;
    private String rcvr_name;
    private String topic_name;
    
    public KafkaReceiverInfo() {
    }
    
    public KafkaReceiverInfo(final String name, final String s) {
        super(name, null);
        this.rcvr_name = name;
        this.topic_name = s;
    }
    
    public String toString() {
        return this.rcvr_name + " on topic " + this.topic_name;
    }
    
    public boolean equals(final Object object) {
        if (!(object instanceof KafkaReceiverInfo)) {
            return false;
        }
        final KafkaReceiverInfo that = (KafkaReceiverInfo)object;
        return this.rcvr_name.equals(that.rcvr_name) && this.topic_name.equals(that.topic_name);
    }
    
    public int hashCode() {
        int result = 31;
        result = 37 * result + ((this.rcvr_name == null) ? 0 : this.rcvr_name.hashCode());
        result = 37 * result + ((this.topic_name == null) ? 0 : this.topic_name.hashCode());
        return result;
    }
    
    public String getRcvr_name() {
        return this.rcvr_name;
    }
    
    public String getTopic_name() {
        return this.topic_name;
    }
    
    @Override
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeString(this.topic_name);
        output.writeString(this.rcvr_name);
    }
    
    @Override
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.topic_name = input.readString();
        this.rcvr_name = input.readString();
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaReceiverInfo.class);
    }
}
