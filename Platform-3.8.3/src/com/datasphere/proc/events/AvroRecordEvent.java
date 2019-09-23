package com.datasphere.proc.events;

import org.apache.avro.generic.*;

import java.util.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:AvroRecordEvent:1.0")
public class AvroRecordEvent extends SimpleEvent
{
    private static final long serialVersionUID = 8049296225793295226L;
    @EventTypeData
    public GenericRecord data;
    public Map<String, Object> metadata;
    
    public AvroRecordEvent() {
    }
    
    public AvroRecordEvent(final long timestamp) {
        super(timestamp);
    }
    
    public GenericRecord getData() {
        return this.data;
    }
    
    public void setData(final GenericRecord record) {
        this.data = record;
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (GenericRecord)payload[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data, this.metadata };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, (Object)this.data);
        kryo.writeClassAndObject(output, (Object)this.metadata);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.data = (GenericRecord)kryo.readClassAndObject(input);
        this.metadata = (Map<String, Object>)kryo.readClassAndObject(input);
    }
}
