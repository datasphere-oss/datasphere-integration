package com.datasphere.appmanager.event;

import java.util.Map;

import com.datasphere.runtime.ActionType;
import com.datasphere.uuid.UUID;

public class ApiCallEvent extends Event
{
    private final Map<String, Object> params;
    private final UUID requestId;
    private final ActionType actionType;
    public static final String DEPLOYMENTPLAN = "DEPLOYMENTPLAN";
    public static final String RECOVERYDESCRIPTION = "RECOVERYDESCRIPTION";
    public static final String COMMAND_TIMESTAMP = "COMMAND_TIMESTAMP";
    
    public ApiCallEvent(final UUID requestId, final ActionType action, final Map<String, Object> params, final UUID flowId) {
        super(flowId);
        this.params = params;
        this.requestId = requestId;
        this.actionType = action;
    }
    
    public Map<String, Object> getParams() {
        return this.params;
    }
    
    public UUID getRequestId() {
        return this.requestId;
    }
    
    public ActionType getActionType() {
        return this.actionType;
    }
    
    @Override
    public EventAction getEventAction() {
        switch (this.actionType) {
            case DEPLOY: {
                return EventAction.API_DEPLOY;
            }
            case START: {
                return EventAction.API_START;
            }
            case STOP: {
                return EventAction.API_STOP;
            }
            case QUIESCE: {
                return EventAction.API_QUIESCE;
            }
            case CHECKPOINT: {
                return EventAction.API_CHECKPOINT;
            }
            case MEMORY_STATUS: {
                return EventAction.API_MEMORY_STATUS;
            }
            case UNDEPLOY: {
                return EventAction.API_UNDEPLOY;
            }
            case RESUME: {
                return EventAction.API_RESUME;
            }
            case STATUS: {
                return EventAction.API_STATUS;
            }
            default: {
                throw new IllegalStateException("Unknown command.");
            }
        }
    }
    
    @Override
    public String toString() {
        return "Request Id: " + this.requestId + " actionType: " + this.actionType;
    }
}
