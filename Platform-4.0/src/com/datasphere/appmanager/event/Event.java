package com.datasphere.appmanager.event;

import java.io.*;
import com.datasphere.uuid.*;

public abstract class Event implements Serializable
{
    private final UUID flowId;
    
    public Event(final UUID flowId) {
        this.flowId = flowId;
    }
    
    public UUID getFlowId() {
        return this.flowId;
    }
    
    public abstract EventAction getEventAction();
    
    public enum EventAction
    {
        NODE_APP_DEPLOYED(false), 
        NODE_APP_STOPPED(false), 
        NODE_APP_QUIESCED(false), 
        NODE_APP_QUIESCE_FLUSHED(true), 
        NODE_APP_QUIESCE_CHECKPOINTED(true), 
        NODE_APP_CHECKPOINTED(true), 
        NODE_PAUSED_SOURCES(false), 
        NODE_APPROVED_QUIESCE(false), 
        NODE_CACHE_DEPLOYED(false), 
        NODE_RUNNING(false), 
        NODE_ERROR(false), 
        NODE_ADDED(false), 
        NODE_DELETED(false), 
        AGENT_NODE_ADDED(false), 
        AGENT_NODE_DELETED(false), 
        API_DEPLOY(false), 
        API_START(false), 
        API_STOP(false), 
        API_QUIESCE(true), 
        API_MEMORY_STATUS(true), 
        API_UNDEPLOY(false), 
        API_RESUME(false), 
        API_STATUS(false), 
        API_CHECKPOINT(true), 
        SOFT_DEPLOY(false), 
        SOFT_START(false), 
        SOFT_STOP(false), 
        SOFT_UNDEPLOY(false), 
        SOFT_CACHE_DEPLOY(false), 
        SOFT_ERROR(false);
        
        private final boolean sendToAgents;
        
        private EventAction(final boolean sendToAgents) {
            this.sendToAgents = sendToAgents;
        }
        
        public boolean sendToAgents() {
            return this.sendToAgents;
        }
    }
}
