package com.datasphere.runtime.monitor;

import java.util.*;

public interface Monitorable
{
    Collection<MonitorEvent> getMonitorEvents(final long p0);
}
