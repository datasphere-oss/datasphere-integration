package com.datasphere.runtime.channels;

import com.datasphere.runtime.components.*;
import java.io.*;
import com.datasphere.runtime.monitor.*;
import java.util.*;
import com.datasphere.runtime.containers.*;

public class SimpleChannel implements Channel
{
    private Link sub;
    
    public SimpleChannel() {
        this.sub = null;
    }
    
    @Override
    public void close() throws IOException {
        this.sub = null;
    }
    
    @Override
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        return Collections.emptyList();
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        if (this.sub != null) {
            this.sub.subscriber.receive(this.sub.linkID, event);
        }
    }
    
    @Override
    public void addSubscriber(final Link link) {
        if (this.sub != null) {
            throw new RuntimeException("SimpleChannel can have only one subscriber");
        }
        this.sub = link;
    }
    
    @Override
    public void removeSubscriber(final Link link) {
        if (this.sub != null && this.sub.equals(link)) {
            this.sub = null;
        }
    }
    
    @Override
    public void addCallback(final NewSubscriberAddedCallback callback) {
        throw new RuntimeException("SimpleChannel cannot have callback");
    }
    
    @Override
    public int getSubscribersCount() {
        return (this.sub != null) ? 1 : 0;
    }
}
