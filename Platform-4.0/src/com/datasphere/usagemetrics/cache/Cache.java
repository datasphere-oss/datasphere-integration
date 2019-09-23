package com.datasphere.usagemetrics.cache;

import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;

public final class Cache
{
    private static final HazelcastInstance hazelcastInstance;
    private static final Map<UUID, UsageMetrics> usageMap;
    private static final Logger logger;
    
    public static void addUsageMetrics(final com.datasphere.usagemetrics.persistence.jpa.UsageMetrics usageMetrics) {
        final UUID cacheKey = new UUID(usageMetrics.getSourceId());
        final UsageMetrics cachedMetrics = getCachedMetrics(cacheKey);
        cachedMetrics.addMetrics(usageMetrics);
        Cache.usageMap.put(cacheKey, cachedMetrics);
    }
    
    public static UsageMetrics getCachedMetrics(final UUID cacheKey) {
        UsageMetrics usageMetrics = Cache.usageMap.get(cacheKey);
        if (usageMetrics == null || usageMetrics.isExpired()) {
            usageMetrics = new UsageMetrics();
        }
        return usageMetrics;
    }
    
    static {
        hazelcastInstance = Cluster.getHazelcastInstance();
        usageMap = (Map)Cache.hazelcastInstance.getMap("UsageMetricsMap");
        logger = Logger.getLogger((Class)Cache.class);
    }
}
