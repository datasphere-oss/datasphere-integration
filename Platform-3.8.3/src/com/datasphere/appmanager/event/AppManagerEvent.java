package com.datasphere.appmanager.event;

import com.datasphere.runtime.*;
import com.datasphere.uuid.*;

public class AppManagerEvent extends Event
{
    private final EventAction eventAction;
    private final ExceptionEvent exceptionEvent;
    
    public AppManagerEvent(final UUID flowId, final EventAction eventAction) {
        super(flowId);
        this.eventAction = eventAction;
        this.exceptionEvent = null;
    }
    
    public AppManagerEvent(final UUID flowId, final EventAction eventAction, final ExceptionEvent exceptionEvent) {
        super(flowId);
        this.eventAction = eventAction;
        this.exceptionEvent = exceptionEvent;
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
        return " FlowId: " + this.getFlowId() + " Action: " + this.eventAction + " exceptionEvent: " + this.exceptionEvent;
    }
}
