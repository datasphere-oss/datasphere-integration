package com.datasphere.proc.events;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:ObjectArrayEvent:1.0")
public class ObjectArrayEvent extends SimpleEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    @EventTypeData
    public Object[] data;
    
    public ObjectArrayEvent() {
    }
    
    public ObjectArrayEvent(final long timestamp) {
        super(timestamp);
    }
    
    public Object[] getData() {
        return this.data;
    }
    
    public void setData(final Object[] data) {
        this.data = data.clone();
    }
    
    public void setPayload(final Object[] payload) {
        this.data = payload.clone();
    }
    
    public Object[] getPayload() {
        return this.data;
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        if (this.data == null) {
            output.write(0);
        }
        else {
            output.write(1);
            output.writeInt(this.data.length);
            for (final Object o : this.data) {
                kryo.writeClassAndObject(output, o);
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final int nullOrNot = input.read();
        if (nullOrNot != 0) {
            final int len = input.readInt();
            this.data = new Object[len];
            for (int i = 0; i < len; ++i) {
                this.data[i] = kryo.readClassAndObject(input);
            }
        }
    }
}
