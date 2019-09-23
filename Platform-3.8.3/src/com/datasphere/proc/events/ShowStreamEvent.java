package com.datasphere.proc.events;

import java.io.Serializable;

import com.datasphere.proc.records.Record;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ShowStreamEvent implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = -4422795284888376503L;
    public String streamName;
    public UUID streamUUID;
    public DARecord event;
    
    public ShowStreamEvent() {
    }
    
    public ShowStreamEvent(final MetaInfo.Stream streamInfo, final DARecord event) {
        this.streamName = streamInfo.uri;
        this.streamUUID = streamInfo.uuid;
        this.event = event;
    }
    
    public String getStreamName() {
        return this.streamName;
    }
    
    public void setStreamName(final String streamName) {
        this.streamName = streamName;
    }
    
    public UUID getStreamUUID() {
        return this.streamUUID;
    }
    
    public void setStreamUUID(final UUID streamUUID) {
        this.streamUUID = streamUUID;
    }
    
    public DARecord getEvent() {
        return this.event;
    }
    
    public void setEvent(final DARecord event) {
        this.event = event;
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeString(this.streamName);
        if (this.streamUUID != null) {
            output.writeByte(0);
            kryo.writeClassAndObject(output, (Object)this.streamUUID);
        }
        else {
            output.writeByte(1);
        }
        if (this.event != null) {
            output.writeByte(0);
            kryo.writeClassAndObject(output, (Object)this.event);
        }
        else {
            output.writeByte(1);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.streamName = input.readString();
        final byte hasStreamUUID = input.readByte();
        if (hasStreamUUID == 0) {
            this.streamUUID = (UUID)kryo.readClassAndObject(input);
        }
        else {
            this.streamUUID = null;
        }
        final byte hasEvent = input.readByte();
        if (hasEvent == 0) {
            this.event = (DARecord)kryo.readClassAndObject(input);
        }
        else {
            this.event = null;
        }
    }
}
