package com.datasphere.runtime.monitor;

import java.io.*;
import java.util.*;

import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class MonitorBatchEvent extends SimpleEvent implements Serializable
{
    private static final long serialVersionUID = 8522984791771979498L;
    public List<MonitorEvent> events;
    
    public MonitorBatchEvent() {
        this.events = null;
    }
    
    public MonitorBatchEvent(final long timestamp) {
        super(timestamp);
        this.events = null;
    }
    
    public MonitorBatchEvent(final long timestamp, final List<MonitorEvent> events) {
        super(timestamp);
        this.events = null;
        this.events = events;
    }
    
    public void setPayload(final Object[] payload) {
        if (payload != null) {
            this.events = new ArrayList<MonitorEvent>(payload.length);
            for (final Object obj : payload) {
                if (obj instanceof MonitorEvent) {
                    this.events.add((MonitorEvent)obj);
                }
            }
        }
    }
    
    public Object[] getPayload() {
        return (Object[])((this.events == null) ? null : this.events.toArray());
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, (Object)this.events);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.events = (List<MonitorEvent>)kryo.readClassAndObject(input);
    }
}
