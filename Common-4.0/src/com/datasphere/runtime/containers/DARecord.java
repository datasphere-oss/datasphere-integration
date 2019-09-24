package com.datasphere.runtime.containers;

import java.io.*;

import com.datasphere.event.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

import java.util.*;

public class DARecord implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = -6202211360415942342L;
    public Object data;
    public Position position;
    public LagMarker lagMarker;
    
    public DARecord() {
        this.position = null;
    }
    
    public DARecord(final Object data) {
        this.position = null;
        this.data = data;
    }
    
    public DARecord(final Object data, final Position pos) {
        this(data);
        this.position = pos;
    }
    
    public void initValues(final DARecord that) {
        this.data = that.data;
        this.position = that.position;
    }
    
    @Override
    public String toString() {
        return this.data.toString();
    }
    
    public void setLagMarker(final LagMarker lagMarker) {
        this.lagMarker = lagMarker;
    }
    
    public LagMarker getLagMarker() {
        return this.lagMarker;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.lagMarker != null) {
            output.writeByte((byte)1);
            kryo.writeClassAndObject(output, (Object)this.lagMarker);
        }
        else {
            output.writeByte((byte)0);
        }
        kryo.writeClassAndObject(output, this.data);
        output.writeBoolean(this.position != null);
        if (this.position != null) {
            output.writeInt(this.position.size());
            for (final Path p : this.position.values()) {
                output.writeInt(p.pathComponentCount());
                for (int c = 0; c < p.pathComponentCount(); ++c) {
                    final Path.Item item = p.getPathComponent(c);
                    kryo.writeClassAndObject(output, (Object)item);
                }
                kryo.writeClassAndObject(output, (Object)p.getLowSourcePosition());
                kryo.writeClassAndObject(output, (Object)p.getHighSourcePosition());
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        final byte response = input.readByte();
        if (response == 1) {
            this.lagMarker = (LagMarker)kryo.readClassAndObject(input);
        }
        this.data = kryo.readClassAndObject(input);
        final boolean positionNotNull = input.readBoolean();
        if (positionNotNull) {
            final Set<Path> paths = new HashSet<Path>();
            for (int numPaths = input.readInt(), c = 0; c < numPaths; ++c) {
                final int numUuids = input.readInt();
                final Path.Item[] items = new Path.Item[numUuids];
                for (int u = 0; u < numUuids; ++u) {
                    final Path.Item item = (Path.Item)kryo.readClassAndObject(input);
                    items[u] = item;
                }
                final Path.ItemList pathItems = Path.ItemList.get(items);
                final SourcePosition lowSourcePosition = (SourcePosition)kryo.readClassAndObject(input);
                final SourcePosition highSourcePosition = (SourcePosition)kryo.readClassAndObject(input);
                final Path path = new Path(pathItems, lowSourcePosition, highSourcePosition);
                paths.add(path);
            }
            this.position = new Position(paths);
        }
    }
    
    @Override
    public int hashCode() {
        if (this.data instanceof SimpleEvent) {
            final SimpleEvent e = (SimpleEvent)this.data;
            return Arrays.hashCode(e.payload);
        }
        return this.data.hashCode();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof DARecord)) {
            return false;
        }
        final DARecord e = (DARecord)o;
        if (this.data instanceof SimpleEvent && e.data instanceof SimpleEvent) {
            final SimpleEvent e2 = (SimpleEvent)this.data;
            final SimpleEvent e3 = (SimpleEvent)e.data;
            return Arrays.equals(e2.payload, e3.payload);
        }
        return (this.data == null) ? (e.data == null) : this.data.equals(e.data);
    }
}
