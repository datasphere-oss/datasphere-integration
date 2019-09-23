package com.datasphere.appmanager.event;

import com.datasphere.uuid.*;

public class CommandConfirmationEvent extends Event
{
    private final UUID serverId;
    private final EventAction eventAction;
    private final Long commandTimestamp;
    
    public CommandConfirmationEvent(final UUID serverId, final UUID flowId, final EventAction eventAction, final Long commandTimestamp) {
        super(flowId);
        this.serverId = serverId;
        this.eventAction = eventAction;
        this.commandTimestamp = commandTimestamp;
    }
    
    public UUID getServerId() {
        return this.serverId;
    }
    
    @Override
    public EventAction getEventAction() {
        return this.eventAction;
    }
    
    public Long getCommandTimestamp() {
        return this.commandTimestamp;
    }
    
    @Override
    public String toString() {
        return "ServerId: " + this.serverId + " FlowId: " + this.getFlowId() + " Action: " + this.eventAction + " commandTimestamp: " + this.commandTimestamp;
    }
}
