package com.datasphere.proc.events;

import org.joda.time.*;

import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class ClusterTestEvent extends SimpleEvent
{
    private static final long serialVersionUID = -4518801128514134357L;
    public String keyValue;
    public DateTime ts;
    public int intValue;
    public double doubleValue;
    
    public ClusterTestEvent() {
    }
    
    public ClusterTestEvent(final long timestamp) {
        super(timestamp);
    }
    
    public String getKeyValue() {
        return this.keyValue;
    }
    
    public void setKeyValue(final String keyValue) {
        this.keyValue = keyValue;
    }
    
    public DateTime getTs() {
        return this.ts;
    }
    
    public void setTs(final DateTime ts) {
        this.ts = ts;
    }
    
    public int getIntValue() {
        return this.intValue;
    }
    
    public void setIntValue(final int intValue) {
        this.intValue = intValue;
    }
    
    public double getDoubleValue() {
        return this.doubleValue;
    }
    
    public void setDoubleValue(final double doubleValue) {
        this.doubleValue = doubleValue;
    }
    
    public void setPayload(final Object[] payload) {
        if (payload != null) {
            this.keyValue = (String)payload[0];
            this.ts = (DateTime)payload[1];
            if (payload[2] != null) {
                this.intValue = (int)payload[2];
            }
            if (payload[3] != null) {
                this.doubleValue = (double)payload[3];
            }
        }
    }
    
    public Object[] getPayload() {
        return new Object[] { this.keyValue, this.ts, this.intValue, this.doubleValue };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeString(this.keyValue);
        kryo.writeClassAndObject(output, (Object)this.ts);
        output.writeInt(this.intValue);
        output.writeDouble(this.doubleValue);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.keyValue = input.readString();
        this.ts = (DateTime)kryo.readClassAndObject(input);
        this.intValue = input.readInt();
        this.doubleValue = input.readDouble();
    }
}
