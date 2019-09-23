package com.datasphere.proc.events;

import flexjson.*;
import com.fasterxml.jackson.annotation.*;
import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:StringArrayEvent:1.0")
public class StringArrayEvent extends SimpleEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    @EventTypeData
    public String[] data;
    
    public StringArrayEvent() {
    }
    
    public StringArrayEvent(final long timestamp) {
        super(timestamp);
    }
    
    public String[] getData() {
        return this.data;
    }
    
    public void setData(final String[] data) {
        this.data = data.clone();
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (String[])payload[0];
    }
    
    @JSON(include = false)
    @JsonIgnore
    public Object[] getPayload() {
        return new Object[] { this.data };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeBoolean(this.data != null);
        if (this.data != null) {
            output.writeInt(this.data.length);
            for (final String s : this.data) {
                output.writeString(s);
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final boolean hasData = input.readBoolean();
        if (hasData) {
            final int len = input.readInt();
            this.data = new String[len];
            for (int i = 0; i < len; ++i) {
                this.data[i] = input.readString();
            }
        }
    }
}
