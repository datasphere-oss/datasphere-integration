package com.datasphere.event;

import com.datasphere.event.SimpleEvent;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class QueryResultEvent extends SimpleEvent
{
    private static final long serialVersionUID = -7225126644053717038L;
    public String[] fieldsInfo;
    
    public QueryResultEvent() {
    }
    
    public QueryResultEvent(final long timestamp) {
        super(timestamp);
    }
    
    public void setFieldsInfo(final String[] fieldsInfo) {
        this.fieldsInfo = fieldsInfo.clone();
    }
    
    public String[] getFieldsInfo() {
        return this.fieldsInfo;
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        if (this.fieldsInfo == null) {
            output.writeByte((byte)0);
        }
        else {
            output.writeByte((byte)1);
            output.writeInt(this.fieldsInfo.length);
            for (final String str : this.fieldsInfo) {
                output.writeString(str);
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        if (input.readByte() == 0) {
            this.fieldsInfo = null;
        }
        else {
            final int size = input.readInt();
            this.fieldsInfo = new String[size];
            for (int i = 0; i < size; ++i) {
                this.fieldsInfo[i] = input.readString();
            }
        }
    }
    
    public void setDataPoints(final Object[] dataPoints) {
    }
    
    public Object[] getDataPoints() {
        return this.payload;
    }
}
