package com.datasphere.source.intf;

import com.datasphere.runtime.monitor.*;

public interface MonitorableComponent
{
    void publishMonitorEvents(final MonitorEventsCollection p0);
}
