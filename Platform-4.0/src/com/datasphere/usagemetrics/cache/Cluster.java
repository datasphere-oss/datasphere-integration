package com.datasphere.usagemetrics.cache;

import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;

public final class Cluster
{
    private static final HazelcastInstance hazelcastInstance;
    
    public static HazelcastInstance getHazelcastInstance() {
        return Cluster.hazelcastInstance;
    }
    
    static {
        hazelcastInstance = HazelcastSingleton.get();
    }
}
