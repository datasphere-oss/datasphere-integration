package com.datasphere.runtime.containers;

import java.io.*;

import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.datasphere.runtime.*;

public interface ITaskEvent extends Serializable, KryoSerializable
{
    boolean snapshotUpdate();
    
    IBatch<DARecord> batch();
    
    IBatch<DARecord> filterBatch(final int p0, final RecordKey p1);
    
    IBatch<DARecord> removedBatch();
    
    IRange snapshot();
    
    int getFlags();
    
    UUID getQueryID();
    
    void setQueryID(final UUID p0);
    
    boolean isLagRecord();
    
    boolean canBeLagEvent();
    
    default EVENTTYPE getEventType() {
        return EVENTTYPE.Default;
    }
    
    public enum EVENTTYPE
    {
        Default, 
        CommandEvent;
    }
}
