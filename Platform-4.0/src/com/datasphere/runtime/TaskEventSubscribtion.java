package com.datasphere.runtime;

import java.util.concurrent.*;
import com.datasphere.runtime.containers.*;

public class TaskEventSubscribtion
{
    private final int userStreamID;
    private final BlockingQueue<TaskEventWithStreamID> channel;
    
    public TaskEventSubscribtion(final int id, final BlockingQueue<TaskEventWithStreamID> channel) {
        this.userStreamID = id;
        this.channel = channel;
    }
    
    public boolean offer(final ITaskEvent e) {
        return this.channel.offer(new TaskEventWithStreamID(this.userStreamID, e));
    }
    
    public void put(final ITaskEvent e) throws InterruptedException {
        this.channel.put(new TaskEventWithStreamID(this.userStreamID, e));
    }
}
