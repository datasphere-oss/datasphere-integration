package com.datasphere.appmanager;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.uuid.UUID;

public class AppDetail implements Serializable, Cloneable
{
    private final UUID serverId;
    private Status localStatus;
    Set<ExceptionEvent> exceptionEvents;
    
    public AppDetail(final UUID serverID, final Status localStatus) {
        this.exceptionEvents = new HashSet<ExceptionEvent>();
        this.serverId = serverID;
        this.localStatus = localStatus;
    }
    
    public AppDetail(final AppDetail newDetail) {
        this(newDetail.serverId, newDetail.localStatus);
        this.exceptionEvents.addAll(newDetail.exceptionEvents);
    }
    
    public UUID getServerId() {
        return this.serverId;
    }
    
    public Status getLocalStatus() {
        return this.localStatus;
    }
    
    public void setLocalStatus(final Status localStatus) {
        this.localStatus = localStatus;
    }
    
    public void addExceptionEvent(final ExceptionEvent e) {
        if (this.localStatus != Status.ERROR) {
            this.localStatus = Status.ERROR;
        }
        this.exceptionEvents.add(e);
    }
    
    public Set<ExceptionEvent> getExceptionEvents() {
        return this.exceptionEvents;
    }
    
    public void resetExceptionEvents() {
        this.exceptionEvents.clear();
    }
    
    public enum Status
    {
        UNKNOWN, 
        UNMANAGED, 
        DEPLOYED, 
        RUNNING, 
        ERROR, 
        DELETED;
    }
}
