package com.datasphere.kafka;

import java.nio.*;

import com.datasphere.recovery.*;
import com.datasphere.runtime.containers.*;

import java.util.*;
/*
 * 
 */
public abstract class PositionedBuffer
{
    public final int partitionId;
    protected ByteBuffer byteBuffer;
    public PathManager recordsInMemory;
    public PathManager waitPosition;
    protected long lastSuccessfulWriteOffset;
    public volatile boolean isFull;
    protected int pendingCount;
    protected long waitStats;
    protected boolean isRecoveryEnabled;
    
    public PositionedBuffer(final int partition_id, final int sendBufferSize) {
        this.recordsInMemory = new PathManager();
        this.waitPosition = null;
        this.lastSuccessfulWriteOffset = 0L;
        this.isFull = false;
        this.pendingCount = 0;
        this.waitStats = 0L;
        this.isRecoveryEnabled = false;
        this.partitionId = partition_id;
        this.byteBuffer = ByteBuffer.allocate(sendBufferSize);
    }
    
    public void setRecoveryEnabled(final boolean isRecoveryEnabled) {
        this.isRecoveryEnabled = isRecoveryEnabled;
    }
    
    public abstract void put(final DARecord p0) throws Exception;
    
    public abstract void put(final ITaskEvent p0) throws Exception;
    
    protected abstract byte[] convertToBytes(final Object p0) throws Exception;
    
    public abstract void flushToKafka() throws Exception;
    
    protected synchronized boolean isBeforeWaitPosition(final Position position) {
        if (this.waitPosition == null || position == null) {
            return false;
        }
        for (final Path datallPath : position.values()) {
            if (!this.waitPosition.containsKey(datallPath.getPathHash())) {
                continue;
            }
            final SourcePosition sp = this.waitPosition.get(datallPath.getPathHash()).getLowSourcePosition();
            if (datallPath.getLowSourcePosition().compareTo(sp) <= 0) {
                return true;
            }
            this.waitPosition.removePath(datallPath.getPathHash());
        }
        if (this.waitPosition.isEmpty()) {
            this.waitPosition = null;
        }
        return false;
    }
    
    public abstract void closeProducer();
}
