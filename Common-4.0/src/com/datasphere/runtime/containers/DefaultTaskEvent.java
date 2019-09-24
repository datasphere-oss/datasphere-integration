package com.datasphere.runtime.containers;

import com.datasphere.runtime.*;
import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class DefaultTaskEvent implements ITaskEvent
{
    private DARecord containedEvent;
    private UUID queryId;
    
    public DefaultTaskEvent(final DARecord xevent) {
        this.queryId = null;
        this.containedEvent = xevent;
    }
    
    public void write(final Kryo kryo, final Output output) {
        kryo.writeClassAndObject(output, (Object)this.containedEvent);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.containedEvent = (DARecord)kryo.readClassAndObject(input);
    }
    
    @Override
    public boolean snapshotUpdate() {
        return this.batch().isEmpty() && this.removedBatch().isEmpty();
    }
    
    @Override
    public IBatch batch() {
        return new DefaultBatch(this.containedEvent);
    }
    
    @Override
    public IBatch removedBatch() {
        return DefaultBatch.EMPTY_BATCH;
    }
    
    @Override
    public IRange snapshot() {
        return DefaultRange.EMPTY_RANGE;
    }
    
    @Override
    public int getFlags() {
        return 1;
    }
    
    @Override
    public UUID getQueryID() {
        return this.queryId;
    }
    
    @Override
    public void setQueryID(final UUID queryID) {
        this.queryId = queryID;
    }
    
    @Override
    public boolean isLagRecord() {
        return false;
    }
    
    @Override
    public boolean canBeLagEvent() {
        return false;
    }
    
    @Override
    public IBatch filterBatch(final int indexID, final RecordKey key) {
        return DefaultBatch.EMPTY_BATCH;
    }
}
