package com.datasphere.appmanager.event;

import java.util.*;
import com.datasphere.uuid.*;

public class BatchNodeAddEvent extends Event
{
    List<UUID> addedNodes;
    private final EventAction eventAction;
    
    public BatchNodeAddEvent(final List<UUID> serverIds, final EventAction eventAction) {
        super(null);
        this.addedNodes = serverIds;
        this.eventAction = eventAction;
    }
    
    @Override
    public EventAction getEventAction() {
        return this.eventAction;
    }
    
    public List<UUID> getMembers() {
        return this.addedNodes;
    }
    
    @Override
    public String toString() {
        return "Added Nodes: " + this.addedNodes + " FlowId: " + this.getFlowId() + " Action: " + this.eventAction;
    }
}
