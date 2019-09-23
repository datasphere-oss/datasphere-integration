package com.datasphere.runtime;

import com.datasphere.runtime.containers.*;

public interface DistributedSubscriber
{
    void receive(final DistLink p0, final ITaskEvent p1);
}
