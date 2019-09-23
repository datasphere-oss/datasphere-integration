package com.datasphere.intf;

import com.datasphere.event.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.uuid.*;

public interface EventSink
{
    void receive(final int p0, final Event p1) throws Exception;
    
    void receive(final int p0, final Event p1, final Position p2) throws Exception;
    
    void receive(final ITaskEvent p0) throws Exception;
    
    UUID getNodeID();
}
