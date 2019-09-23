package com.datasphere.runtime;

import java.util.concurrent.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

public class TaskEventSubscribtionManager
{
    private final List<TaskEventSubscribtion> subscribers;
    
    public TaskEventSubscribtionManager() {
        this.subscribers = new CopyOnWriteArrayList<TaskEventSubscribtion>();
    }
    
    public void addSubscribtion(final TaskEventSubscribtion s) {
        this.subscribers.add(s);
    }
    
    public boolean removeSubscribtion(final TaskEventSubscribtion s) {
        return this.subscribers.remove(s);
    }
    
    public void putTaskEvent(final ITaskEvent e) throws InterruptedException {
        for (final TaskEventSubscribtion sub : this.subscribers) {
            sub.put(e);
        }
    }
    
    public int offerTaskEvent(final ITaskEvent e) {
        int notReceived = 0;
        for (final TaskEventSubscribtion sub : this.subscribers) {
            if (!sub.offer(e)) {
                ++notReceived;
            }
        }
        return notReceived;
    }
}
