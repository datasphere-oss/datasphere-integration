package com.datasphere.proc.events;

import com.datasphere.uuid.*;
import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:DynamicEvent:1.0")
public class DynamicEvent extends SimpleEvent
{
    private static final long serialVersionUID = -7172644088275463812L;
    private UUID eventType;
    @EventTypeData
    public Object[] data;
    
    public DynamicEvent() {
    }
    
    public DynamicEvent(final long timestamp) {
        super(timestamp);
    }
    
    public DynamicEvent(final UUID eventType) {
        super(System.currentTimeMillis());
        this.eventType = eventType;
    }
    
    public void setData(final String[] data) {
        this.data = data.clone();
    }
    
    public Object[] getData() {
        return this.data;
    }
    
    public UUID getEventType() {
        return this.eventType;
    }
    
    public void setPayload(final Object[] data) {
        this.data = (Object[])data[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, (Object)this.eventType);
        kryo.writeClassAndObject(output, (Object)this.data);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.eventType = (UUID)kryo.readClassAndObject(input);
        this.data = (Object[])kryo.readClassAndObject(input);
    }
}
