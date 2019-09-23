package com.datasphere.appmanager.event;

import com.datasphere.uuid.*;
import com.datasphere.runtime.*;

public class NodeEvent extends Event
{
    private final UUID serverId;
    private final EventAction eventAction;
    private final ExceptionEvent exceptionEvent;
    
    public NodeEvent(final UUID serverId, final UUID flowId, final EventAction eventAction) {
        super(flowId);
        this.serverId = serverId;
        this.eventAction = eventAction;
        this.exceptionEvent = null;
    }
    
    public NodeEvent(final UUID serverId, final UUID flowId, final EventAction eventAction, final ExceptionEvent exceptionEvent) {
        super(flowId);
        this.serverId = serverId;
        this.eventAction = eventAction;
        this.exceptionEvent = exceptionEvent;
    }
    
    public UUID getServerId() {
        return this.serverId;
    }
    
    @Override
    public EventAction getEventAction() {
        return this.eventAction;
    }
    
    public ExceptionEvent getExceptionEvent() {
        return this.exceptionEvent;
    }
    
    @Override
    public String toString() {
        return "ServerId: " + this.serverId + " FlowId: " + this.getFlowId() + " Action: " + this.eventAction + " exceptionEvent: " + this.exceptionEvent;
    }
}
