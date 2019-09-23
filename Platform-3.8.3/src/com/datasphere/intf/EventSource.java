package com.datasphere.intf;

public interface EventSource
{
    void addEventSink(final EventSink p0);
    
    void removeEventSink(final EventSink p0);
}
