package com.datasphere.runtime;

import com.lmax.disruptor.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.recovery.*;

public class StreamEventFactory implements EventFactory<EventContainer>
{
    public static ITaskEvent createStreamEvent(final Object data) {
        return createStreamEvent(data, new Position());
    }
    
    public static ITaskEvent createStreamEvent(final Object data, final Position pos) {
        return (ITaskEvent)new StreamTaskEvent(data, pos);
    }
    
    public EventContainer newInstance() {
        return new EventContainer();
    }
}
