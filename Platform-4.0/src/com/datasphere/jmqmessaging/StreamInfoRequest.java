package com.datasphere.jmqmessaging;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class StreamInfoRequest implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = 5676452220816985613L;
    private String streamName;
    
    public StreamInfoRequest() {
    }
    
    public StreamInfoRequest(final String stream) {
        this.streamName = stream;
    }
    
    public String getStreamName() {
        return this.streamName;
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeString(this.streamName);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.streamName = input.readString();
    }
    
    @Override
    public String toString() {
        return "StreaminfoReqeust for " + this.streamName;
    }
}
