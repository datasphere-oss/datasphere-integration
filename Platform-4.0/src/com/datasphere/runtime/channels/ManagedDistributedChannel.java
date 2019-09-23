package com.datasphere.runtime.channels;

import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.components.*;
import java.io.*;
import java.util.*;
import com.datasphere.runtime.monitor.*;

public class ManagedDistributedChannel implements Channel
{
    private final BaseServer server;
    private final Stream stream_runtime;
    
    public ManagedDistributedChannel(final BaseServer server, final Stream stream_runtime) {
        this.server = server;
        this.stream_runtime = stream_runtime;
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
    }
    
    @Override
    public void addSubscriber(final Link link) {
    }
    
    @Override
    public void removeSubscriber(final Link link) {
    }
    
    @Override
    public void addCallback(final NewSubscriberAddedCallback callback) {
    }
    
    @Override
    public int getSubscribersCount() {
        return 0;
    }
    
    @Override
    public void close() throws IOException {
    }
    
    @Override
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        return null;
    }
}
