package com.datasphere.runtime.channels;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.MonitorableComponent;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

public class BroadcastChannel extends MonitorableComponent implements Channel
{
    private NewSubscriberAddedCallback callback;
    private final List<Link> links;
    protected final FlowComponent owner;
    protected volatile long received;
    protected volatile long processed;
    
    public BroadcastChannel(final FlowComponent owner) {
        this.links = new CopyOnWriteArrayList<Link>();
        this.received = 0L;
        this.processed = 0L;
        this.owner = owner;
    }
    
    @Override
    public void close() throws IOException {
        this.links.clear();
    }
    
    public void doPublish(final ITaskEvent event) throws Exception {
        for (final Link link : this.links) {
            link.subscriber.receive(link.linkID, event);
        }
        ++this.processed;
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        ++this.received;
        this.doPublish(event);
    }
    
    @Override
    public void addSubscriber(final Link link) {
        if (this.callback != null) {
            this.callback.notifyMe(link);
        }
        this.links.add(link);
    }
    
    @Override
    public void removeSubscriber(final Link link) {
        this.links.remove(link);
    }
    
    @Override
    public void addCallback(final NewSubscriberAddedCallback callback) {
        this.callback = callback;
    }
    
    @Override
    public int getSubscribersCount() {
        return this.links.size();
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection events) {
    }
    
    @Override
    public UUID getMetaID() {
        return this.owner.getMetaID();
    }
    
    @Override
    public BaseServer srv() {
        return this.owner.srv();
    }
    
    public String getOwnerMetaName() {
        return this.owner.getMetaName();
    }
}
