package com.datasphere.proc.events;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class HQueueEvent implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 8283123723215896551L;
    String key;
    Object payload;
    
    public HQueueEvent(final String key, final Object payload) {
        this.key = key;
        this.payload = payload;
    }
    
    public String getKey() {
        return this.key;
    }
    
    public void setKey(final String key) {
        this.key = key;
    }
    
    public Object getPayload() {
        return this.payload;
    }
    
    public void setPayload(final Object payload) {
        this.payload = payload;
    }
    
    @Override
    public String toString() {
        return "WAQueueMsg(" + this.key + " " + (Object)((this.payload instanceof byte[]) ? ((byte[])this.payload).length : this.payload) + ")";
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeString(this.key);
        if (this.payload != null) {
            output.writeByte(0);
            kryo.writeClassAndObject(output, this.payload);
        }
        else {
            output.writeByte(1);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.key = input.readString();
        final byte hasPayload = input.readByte();
        if (hasPayload == 0) {
            this.payload = kryo.readClassAndObject(input);
        }
    }
}
