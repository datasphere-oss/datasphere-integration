package com.datasphere.runtime.monitor;

import java.util.ArrayList;
import java.util.Collection;

import com.datasphere.uuid.UUID;

public class MonitorEventsCollection
{
    private final Collection<MonitorEvent> monEvs;
    private final long timeStamp;
    private final Long prevTimeStamp;
    private final UUID serverID;
    private final UUID entityID;
    
    public MonitorEventsCollection(final long timeStamp, final Long prevTimeStamp, final UUID serverID, final UUID entityID) {
        this.monEvs = new ArrayList<MonitorEvent>();
        this.timeStamp = timeStamp;
        this.prevTimeStamp = prevTimeStamp;
        this.serverID = serverID;
        this.entityID = entityID;
    }
    
    public void add(final MonitorEvent.Type type, final Long value) {
        this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, Long.valueOf(this.timeStamp)));
    }
    
    public void add(final MonitorEvent.Type type, final Long value, final MonitorEvent.Operation operation) {
        this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, this.timeStamp, operation));
    }
    
    public void add(final MonitorEvent event) {
        this.monEvs.add(event);
    }
    
    public void add(final MonitorEvent.Type type, final String value) {
        this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, Long.valueOf(this.timeStamp)));
    }
    
    public void add(final MonitorEvent.Type type, final String value, final MonitorEvent.Operation operation) {
        this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, this.timeStamp, operation));
    }
    
    public Collection<MonitorEvent> getEvents() {
        return this.monEvs;
    }
    
    public long getTimeStamp() {
        return this.timeStamp;
    }
    
    public Long getPrevTimeStamp() {
        return this.prevTimeStamp;
    }
    
    public Long getTimeSpanSecs() {
        if (this.prevTimeStamp == null) {
            return null;
        }
        return (this.timeStamp - this.prevTimeStamp) / 1000L;
    }
    
    public Long getRate(final Long valNow, final Long valPrev) {
        if (valNow == null || valPrev == null) {
            return null;
        }
        final Long timeSpan = this.getTimeSpanSecs();
        if (timeSpan == null) {
            return null;
        }
        return (long)Math.ceil((valNow - valPrev) / timeSpan);
    }
    
    public UUID getServerID() {
        return this.serverID;
    }
    
    public UUID getEntityID() {
        return this.entityID;
    }
}
