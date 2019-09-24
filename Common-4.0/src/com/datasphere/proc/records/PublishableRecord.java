package com.datasphere.proc.records;

import java.io.*;

import com.datasphere.runtime.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class PublishableRecord implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = -5718860713267311590L;
    private short versionId;
    private short partitionId;
    private long authToken;
    private StreamEvent streamEvent;
    
    public PublishableRecord() {
        this(null);
    }
    
    public PublishableRecord(final StreamEvent event) {
        this(event, 0L, (short)0, (short)0);
    }
    
    public PublishableRecord(final StreamEvent event, final long authToken, final short partId, final short verId) {
        this.streamEvent = event;
        this.authToken = authToken;
        this.partitionId = partId;
        this.versionId = verId;
    }
    
    public short getVersionId() {
        return this.versionId;
    }
    
    public short getPartitionId() {
        return this.partitionId;
    }
    
    public long getAuthToken() {
        return this.authToken;
    }
    
    public StreamEvent getStreamEvent() {
        return this.streamEvent;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.streamEvent != null) {
            output.writeBoolean(true);
            kryo.writeClassAndObject(output, (Object)this.streamEvent);
        }
        else {
            output.writeBoolean(false);
        }
        output.writeLong(this.authToken);
        output.writeShort((int)this.partitionId);
        output.writeShort((int)this.versionId);
    }
    
    public void read(final Kryo kryo, final Input input) {
        final boolean hasEvent = input.readBoolean();
        if (hasEvent) {
            this.streamEvent = (StreamEvent)kryo.readClassAndObject(input);
        }
        else {
            this.streamEvent = null;
        }
        this.authToken = input.readLong();
        this.partitionId = input.readShort();
        this.versionId = input.readShort();
    }
    
    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer("PublishableEvent(");
        buffer.append(((this.streamEvent == null) ? "NullEvent" : this.streamEvent.toString()) + " , ");
        buffer.append(this.partitionId + " , ");
        buffer.append(this.versionId + " , ");
        buffer.append(this.authToken + ")");
        return buffer.toString();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof PublishableRecord)) {
            return false;
        }
        final PublishableRecord evt = (PublishableRecord)o;
        boolean ret = false;
        if (this.streamEvent == null) {
            ret = (evt.getStreamEvent() == null);
        }
        else {
            ret = this.streamEvent.equals(evt.getStreamEvent());
        }
        ret &= (this.partitionId == evt.getPartitionId());
        ret &= (this.versionId == evt.getVersionId());
        ret &= (this.authToken == evt.getAuthToken());
        return ret;
    }
}
