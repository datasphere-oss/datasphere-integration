package com.datasphere.usagemetrics;

import com.datasphere.uuid.*;

public final class Constants
{
    public static final UUID PARTITION_ID;
    public static final long MILLISECONDS_PER_SECOND = 1000L;
    public static final long COLLECTOR_CACHE_PERIOD = 2000L;
    public static final long COLLECTOR_PERSIST_PERIOD = 60000L;
    public static final String USAGE_METRICS_MAP = "UsageMetricsMap";
    public static final String PERSISTENCE_UNIT_NAME = "UsageMetrics";
    
    static {
        PARTITION_ID = new UUID(System.currentTimeMillis());
    }
}
