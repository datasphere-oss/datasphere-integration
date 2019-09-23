package com.datasphere.runtime;

import com.datasphere.runtime.containers.*;

public class TaskEventWithStreamID
{
    public final int userStreamID;
    public final ITaskEvent event;
    
    public TaskEventWithStreamID(final int id, final ITaskEvent e) {
        this.userStreamID = id;
        this.event = e;
    }
}
