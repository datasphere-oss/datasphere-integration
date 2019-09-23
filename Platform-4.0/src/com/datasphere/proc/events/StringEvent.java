package com.datasphere.proc.events;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:StringEvent:1.0")
public class StringEvent extends SimpleEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    @EventTypeData
    public String data;
    
    public StringEvent() {
    }
    
    public StringEvent(final long timestamp) {
        super(timestamp);
    }
    
    public String getData() {
        return this.data;
    }
    
    public void setData(final String data) {
        this.data = data;
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (String)payload[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeString(this.data);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.data = input.readString();
    }
}
