package com.datasphere.runtime;

public class EventContainer
{
    private Object event;
    private DistSub distSub;
    private int partitionId;
    private ChannelEventHandler eventHandler;
    
    public EventContainer() {
    }
    
    public EventContainer(final Object event, final UnManagedZMQDistSub distSub, final int partitionId, final ChannelEventHandler channelEventHandler) {
        this.event = event;
        this.distSub = distSub;
        this.partitionId = partitionId;
        this.eventHandler = channelEventHandler;
    }
    
    public void setEventHandler(final ChannelEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }
    
    public ChannelEventHandler getEventHandler() {
        return this.eventHandler;
    }
    
    public void setAllFields(final Object event, final DistSub distSub, final int partitionId, final ChannelEventHandler channelEventHandler) {
        this.setEvent(event);
        this.setEventHandler(channelEventHandler);
        this.setDistSub(distSub);
        this.setPartitionId(partitionId);
    }
    
    public void setEvent(final Object event) {
        this.event = event;
    }
    
    public void setDistSub(final DistSub distSub) {
        this.distSub = distSub;
    }
    
    public void setPartitionId(final int partitionId) {
        this.partitionId = partitionId;
    }
    
    public Object getEvent() {
        return this.event;
    }
    
    public DistSub getDistSub() {
        return this.distSub;
    }
    
    public int getPartitionId() {
        return this.partitionId;
    }
}
