package com.datasphere.kafka;

import com.datasphere.recovery.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

import java.util.*;
import java.io.*;
/*
 * 偏移量位置定义
 */
public class OffsetPosition extends Position
{
    private static final long serialVersionUID = -2092065497461398815L;
    private Offset offset;
    private long timestamp;
    
    public OffsetPosition(final Offset offset, final long timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
    }
    
    public OffsetPosition(final OffsetPosition that) {
        super(that);
        this.offset = that.offset;
        this.timestamp = that.timestamp;
    }
    
    public OffsetPosition(final Position that, final Offset offset, final long timestamp) {
        super(that);
        this.offset = offset;
        this.timestamp = timestamp;
    }
    
    public Offset getOffset() {
        return this.offset;
    }
    
    public void setOffset(final Offset offset) {
        this.offset = offset;
    }
    
    public long getTimestamp() {
        return this.timestamp;
    }
    
    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.offset = (Offset)kryo.readClassAndObject(input);
    }
    
    @Override
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, (Object)this.offset);
    }
    
    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append("[Offset=").append(this.offset).append("; Ts=").append(this.timestamp).append("; ");
        for (final Path p : this.values()) {
            result.append(p);
            result.append(";");
        }
        result.append("]");
        return result.toString();
    }
    
    public String toJson() {
        StringBuilder result = new StringBuilder();
        result.append("\"Offset\":").append(this.offset).append(",\n");
        result.append("\"Paths\":[\n");
        if (!this.values().isEmpty()) {
            for (final Path p : this.values()) {
                result.append(p.toStandardJSON()).append(",\n");
            }
            result = new StringBuilder(result.substring(0, result.lastIndexOf(",")));
        }
        result.append("]\n");
        return result.toString();
    }
}
