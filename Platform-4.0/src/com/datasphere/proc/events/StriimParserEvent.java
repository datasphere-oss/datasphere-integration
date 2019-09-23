package com.datasphere.proc.events;

import java.util.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:StriimParserEvent:1.0")
public class StriimParserEvent extends SimpleEvent
{
    @EventTypeData
    public Object data;
    public Map<String, Object> metadata;
    
    public StriimParserEvent() {
    }
    
    public StriimParserEvent(final long timestamp) {
        super(timestamp);
    }
    
    public void setData(final Object data) {
        this.data = data;
    }
    
    public Object getData() {
        return this.data;
    }
    
    public void setMetadata(final Map<String, Object> metadata) {
        this.metadata = metadata;
    }
    
    public Map<String, Object> getMetadata() {
        return this.metadata;
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeBoolean(this.data != null);
        if (this.data != null) {
            kryo.writeClassAndObject(output, this.data);
        }
        output.writeBoolean(this.metadata != null);
        if (this.metadata != null) {
            kryo.writeClassAndObject(output, (Object)this.metadata);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        if (input.readBoolean()) {
            this.data = kryo.readClassAndObject(input);
        }
        if (input.readBoolean()) {
            this.metadata = (Map<String, Object>)kryo.readClassAndObject(input);
        }
    }
}
