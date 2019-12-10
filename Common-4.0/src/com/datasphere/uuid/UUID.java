package com.datasphere.uuid;

import org.apache.log4j.*;
import flexjson.*;
import com.fasterxml.jackson.annotation.*;
import javax.jdo.annotations.*;
import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import java.nio.*;
/*
 * IMPORTANCE:HIGH
 * UUID作为 数据集成平台全局的唯一标识
 */
public class UUID implements Comparable<UUID>, KryoSerializable, Serializable, Cloneable
{
    private static Logger logger;
    static final long serialVersionUID = 7435962790062944603L;
    public static final String PATTERN = "[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}";
    @JSON(include = false)
    @JsonIgnore
    @Persistent
    public long time;
    @JSON(include = false)
    @JsonIgnore
    @Persistent
    public long clockSeqAndNode;
    private String stringRep;
    
    public UUID() {
        this.stringRep = null;
    }
    
    public UUID(final long currentTimeMillis) {
        this(UUIDGen.createTime(currentTimeMillis), UUIDGen.getClockSeqAndNode());
    }
    
    public UUID(final long time, final long clockSeqAndNode) {
        this.stringRep = null;
        this.time = time;
        this.clockSeqAndNode = clockSeqAndNode;
    }
    
    public UUID(final UUID u) {
        this(u.time, u.clockSeqAndNode);
    }
    
    public UUID(final String uidstr) {
        this.stringRep = null;
        this.setUUIDString(uidstr);
    }
    
    public UUID(final CharSequence s) {
        this(Hex.parseLong(s.subSequence(0, 18)), Hex.parseLong(s.subSequence(19, 36)));
    }
    
    @Override
    public int compareTo(final UUID t) {
        if (this == t) {
            return 0;
        }
        if (this.time > t.time) {
            return 1;
        }
        if (this.time < t.time) {
            return -1;
        }
        if (this.clockSeqAndNode > t.clockSeqAndNode) {
            return 1;
        }
        if (this.clockSeqAndNode < t.clockSeqAndNode) {
            return -1;
        }
        return 0;
    }
    
    @Override
    public final String toString() {
        if (this.stringRep == null) {
            this.stringRep = this.toAppendable(null).toString();
        }
        return this.stringRep;
    }
    
    public StringBuffer toStringBuffer(final StringBuffer in) {
        StringBuffer out = in;
        if (out == null) {
            out = new StringBuffer(36);
        }
        else {
            out.ensureCapacity(out.length() + 36);
        }
        return (StringBuffer)this.toAppendable(out);
    }
    
    public Appendable toAppendable(final Appendable a) {
        Appendable out = a;
        if (out == null) {
            out = new StringBuilder(36);
        }
        try {
            Hex.append(out, (int)(this.time >> 32)).append('-');
            Hex.append(out, (short)(this.time >> 16)).append('-');
            Hex.append(out, (short)this.time).append('-');
            Hex.append(out, (short)(this.clockSeqAndNode >> 48)).append('-');
            Hex.append(out, this.clockSeqAndNode, 12);
        }
        catch (IOException ex) {}
        return out;
    }
    
    @Override
    public int hashCode() {
        return (int)(this.time >> 32 ^ this.time ^ this.clockSeqAndNode >> 32 ^ this.clockSeqAndNode);
    }
    
    public Object clone() {
        try {
            return super.clone();
        }
        catch (CloneNotSupportedException ex) {
            return null;
        }
    }
    
    public final long getTime() {
        return this.time;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public final long getSystemTime() {
        return (this.time - 122192928000000000L) / 10000L;
    }
    
    public final long getClockSeqAndNode() {
        return this.clockSeqAndNode;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UUID)) {
            return false;
        }
        final UUID that = (UUID)obj;
        return this.time == that.time && this.clockSeqAndNode == that.clockSeqAndNode;
    }
    
    public static UUID nilUUID() {
        return new UUID(0L, 0L);
    }
    
    public void read(final Kryo kr, final Input input) {
        this.time = input.readLong();
        this.clockSeqAndNode = input.readLong();
    }
    
    public void write(final Kryo kr, final Output output) {
        output.writeLong(this.time);
        output.writeLong(this.clockSeqAndNode);
    }
    
    public String getUUIDString() {
        return this.toString();
    }
    
    public void setUUIDString(final String uuid) {
        this.time = Hex.parseLong(uuid.subSequence(0, 18));
        this.clockSeqAndNode = Hex.parseLong(uuid.subSequence(19, 36));
    }
    
    public byte[] toBytes() {
        final ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(this.clockSeqAndNode);
        buffer.putLong(this.time);
        return buffer.array();
    }
    
    public byte[] toEightBytes() {
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(this.clockSeqAndNode ^ this.time);
        return buffer.array();
    }
    
    public static UUID genCurTimeUUID() {
        return new UUID(System.currentTimeMillis());
    }
    
    static {
        UUID.logger = Logger.getLogger((Class)UUID.class);
    }
}
