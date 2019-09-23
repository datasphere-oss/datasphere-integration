package com.datasphere.cache;

import javax.cache.configuration.*;
import java.util.*;

public class CacheConfiguration<K, V> extends MutableConfiguration<K, V>
{
    private static final long serialVersionUID = -5567979337927374512L;
    int numReplicas;
    private PartitionManagerType partitionManagerType;
    
    public CacheConfiguration() {
        this.numReplicas = 2;
        this.partitionManagerType = PartitionManagerType.LEGACY;
    }
    
    public CacheConfiguration(final int numReplicas) {
        this.numReplicas = numReplicas;
        this.partitionManagerType = PartitionManagerType.LEGACY;
    }
    
    public CacheConfiguration(final int numReplicas, final PartitionManagerType type) {
        this.numReplicas = numReplicas;
        this.partitionManagerType = type;
    }
    
    public int getNumReplicas() {
        return this.numReplicas;
    }
    
    public void setNumReplicas(final int numReplicas) {
        this.numReplicas = numReplicas;
    }
    
    public PartitionManagerType getPartitionManagerType() {
        return this.partitionManagerType;
    }
    
    public static int getNumReplicasFromProps(final Map<String, Object> props) {
        int numReplicas = 2;
        final Object replication = props.get("replicas");
        if (replication != null) {
            if (replication.toString().equalsIgnoreCase("all") || replication.toString().equalsIgnoreCase("full")) {
                numReplicas = 0;
            }
            else {
                numReplicas = Integer.parseInt(replication.toString());
            }
        }
        return numReplicas;
    }
    
    public enum PartitionManagerType
    {
        SIMPLE, 
        LEGACY;
    }
}
