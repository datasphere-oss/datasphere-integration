package com.datasphere.appmanager;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class ApplicationStatusResponse extends ChangeApplicationStateResponse implements Serializable
{
    private static final long serialVersionUID = -6801388395840353959L;
    MetaInfo.StatusInfo.Status status;
    private final Set<ExceptionEvent> exceptionEvents;
    
    public ApplicationStatusResponse(final UUID requestId, final RESULT result, final String exceptionMsg, final MetaInfo.StatusInfo.Status status, final Set<ExceptionEvent> exceptionEvents) {
        super(requestId, result, exceptionMsg);
        this.exceptionEvents = new HashSet<ExceptionEvent>();
        this.status = status;
        if (exceptionEvents != null) {
            this.exceptionEvents.addAll(exceptionEvents);
        }
    }
    
    public MetaInfo.StatusInfo.Status getStatus() {
        return this.status;
    }
    
    public Set<ExceptionEvent> getExceptionEvents() {
        return this.exceptionEvents;
    }
}
