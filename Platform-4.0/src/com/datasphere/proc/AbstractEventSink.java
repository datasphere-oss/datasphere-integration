package com.datasphere.proc;

import com.datasphere.event.*;
import com.datasphere.intf.*;
import com.datasphere.uuid.*;
import com.datasphere.metaRepository.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.containers.*;

public abstract class AbstractEventSink implements EventSink
{
    @Override
    public UUID getNodeID() {
        return HazelcastSingleton.getNodeId();
    }
    
    @Override
    public void receive(final int channel, final Event event, final Position pos) throws Exception {
        this.receive(channel, event);
    }
    
    @Override
    public void receive(final ITaskEvent batch) throws Exception {
        this.receive(0, null);
    }
}
