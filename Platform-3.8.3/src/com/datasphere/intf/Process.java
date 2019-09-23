package com.datasphere.intf;

import java.util.Map;

import com.datasphere.event.Event;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.uuid.UUID;

public interface Process extends EventSink, EventSource, Worker
{
    void init(final Map<String, Object> p0) throws Exception;
    
    void init(final Map<String, Object> p0, final Map<String, Object> p1) throws Exception;
    
    void init(final Map<String, Object> p0, final Map<String, Object> p1, final UUID p2, final String p3) throws Exception;
    
    void init(final Map<String, Object> p0, final Map<String, Object> p1, final UUID p2, final String p3, final SourcePosition p4, final boolean p5, final Flow p6) throws Exception;
    
    void send(final Event p0, final int p1) throws Exception;
    
    void send(final Event p0, final int p1, final Position p2) throws Exception;
    
    void send(final ITaskEvent p0) throws Exception;
}
