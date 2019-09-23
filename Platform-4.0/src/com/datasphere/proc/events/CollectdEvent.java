package com.datasphere.proc.events;

import org.joda.time.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:CollectdEvent:1.0")
public class CollectdEvent extends SimpleEvent
{
    private static final long serialVersionUID = -4533854701582138286L;
    public String hostName;
    public DateTime time;
    public DateTime timeHighResolution;
    public String pluginName;
    public String pluginInstanceName;
    public String typeName;
    public String typeInstanceName;
    public Long timeInterval;
    public Long intervalHighResolution;
    public String message;
    public Long severity;
    @EventTypeData
    public Object[] data;
    
    public void setPayload(final Object[] data) {
        this.data = data.clone();
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeString(this.hostName);
        kryo.writeClassAndObject(output, (Object)this.time);
        kryo.writeClassAndObject(output, (Object)this.timeHighResolution);
        output.writeString(this.pluginName);
        output.writeString(this.pluginInstanceName);
        output.writeString(this.typeName);
        output.writeString(this.typeInstanceName);
        output.writeLong((long)this.timeInterval);
        output.writeLong((long)this.intervalHighResolution);
        output.writeString(this.message);
        output.writeLong((long)this.severity);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.hostName = input.readString();
        this.time = (DateTime)kryo.readClassAndObject(input);
        this.timeHighResolution = (DateTime)kryo.readClassAndObject(input);
        this.pluginName = input.readString();
        this.pluginInstanceName = input.readString();
        this.typeName = input.readString();
        this.typeInstanceName = input.readString();
        this.timeInterval = input.readLong();
        this.intervalHighResolution = input.readLong();
        this.message = input.readString();
        this.severity = input.readLong();
    }
}
