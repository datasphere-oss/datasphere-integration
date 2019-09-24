package com.datasphere.runtime;

import java.io.*;

import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class DistLink implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 4032047921091787606L;
    private UUID subID;
    private int linkID;
    public String name;
    private int hash;
    
    public DistLink() {
        this.subID = null;
        this.linkID = -1;
        this.name = null;
        this.hash = 0;
    }
    
    public DistLink(final UUID subID, final int linkID, final String name) {
        this.subID = subID;
        this.linkID = linkID;
        this.name = name;
        this.hash = (subID.hashCode() ^ linkID);
    }
    
    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof DistLink)) {
            return false;
        }
        final DistLink other = (DistLink)o;
        return this.getSubID().equals(other.getSubID()) && this.getLinkID() == other.getLinkID();
    }
    
    @Override
    public final int hashCode() {
        return this.hash;
    }
    
    @Override
    public final String toString() {
        return "(" + this.name + ":" + this.subID + "-" + this.linkID + ":" + this.hash + ")";
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.subID != null) {
            output.writeByte(0);
            this.subID.write(kryo, output);
        }
        else {
            output.writeByte(1);
        }
        output.writeInt(this.linkID);
        output.writeString(this.name);
        output.writeInt(this.hash);
    }
    
    public void read(final Kryo kryo, final Input input) {
        final byte hasSubID = input.readByte();
        if (hasSubID == 0) {
            (this.subID = new UUID()).read(kryo, input);
        }
        this.linkID = input.readInt();
        this.name = input.readString();
        this.hash = input.readInt();
    }
    
    public UUID getSubID() {
        return this.subID;
    }
    
    public void setSubID(final UUID subID) {
        this.subID = subID;
    }
    
    public int getLinkID() {
        return this.linkID;
    }
    
    public void setLinkID(final int linkID) {
        this.linkID = linkID;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public int getHash() {
        return this.hash;
    }
    
    public void setHash(final int hash) {
        this.hash = hash;
    }
}
