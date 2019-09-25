package com.datasphere.databasewriter;

import com.datasphere.event.*;
import com.datasphere.recovery.*;

public class EventData
{
    public Event event;
    public Position position;
    public String target;
    
    public EventData(final Event event, final Position position, final String target) {
        this.event = event;
        this.position = position;
        this.target = target;
    }
}
