package com.datasphere.messaging;

import com.datasphere.runtime.*;

public interface InterThreadComm
{
    void put(final Object p0, final DistSub p1, final int p2, final ChannelEventHandler p3) throws InterruptedException;
    
    long size();
    
    void stop();
}
