package com.datasphere.runtime;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

public interface IPartitionManager
{
    void doShutdown();
    
    void setServers(final List<UUID> p0);
    
    int getNumReplicas();
    
    int getPartitionId(final Object p0);
    
    UUID getPartitionOwnerForKey(final Object p0, final int p1);
    
    UUID getPartitionOwnerForPartition(final int p0, final int p1);
    
    UUID getFirstPartitionOwnerForPartition(final int p0);
    
    boolean isLocalPartitionByKey(final Object p0, final int p1);
    
    boolean isLocalPartitionById(final int p0, final int p1);
    
    boolean hasLocalPartitionForId(final int p0);
    
    boolean hasLocalPartitionForKey(final Object p0);
    
    List<UUID> getAllReplicas(final int p0);
    
    boolean isLocalPartitionByUUID(final UUID p0);
    
    UUID getLocal();
    
    boolean isMultiNode();
    
    List<UUID> getPeers();
    
    void addUpdateListener(final ConsistentHashRing.UpdateListener p0);
    
    int getNumberOfPartitions();
}
