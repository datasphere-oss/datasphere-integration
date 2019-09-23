package com.datasphere.runtime.components;

import com.datasphere.runtime.containers.*;

public interface Subscriber
{
    void receive(final Object p0, final ITaskEvent p1) throws Exception;
    
    default boolean canReceiveDataSynchronously() {
        return false;
    }
}
