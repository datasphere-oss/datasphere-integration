package com.datasphere.runtime;

public interface ChannelEventHandler
{
    void sendEvent(final EventContainer p0, final int p1, final LagMarker p2) throws InterruptedException;
}
