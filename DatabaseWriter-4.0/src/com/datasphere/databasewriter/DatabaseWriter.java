package com.datasphere.databasewriter;

import java.util.Map;
import java.util.Observer;

import org.joda.time.DateTime;

import com.datasphere.event.Event;
import com.datasphere.recovery.Position;
import com.datasphere.uuid.UUID;

public interface DatabaseWriter
{
    void initDatabaseWriter(final Map<String, Object> p0, final Map<String, Object> p1, final UUID p2, final String p3) throws Exception;
    
    void processEvent(final int p0, final Event p1, final Position p2) throws Exception;
    
    void closeDatabaseWriter() throws Exception;
    
    void registerObserver(final Observer p0);
    
    Position getWaitPositionFromWriter(final String p0) throws Exception;
    
    DateTime lastCommitTime();
    
    DateTime lastBatcheExecuteTime();
    
    long eventCount();
    
    long executionLatency();
    
    long batchedEventCount();
    
    long eventsInLastCommit();
}
