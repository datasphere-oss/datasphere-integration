package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.List;

import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.uuid.UUID;

public class SimplePartitionManager implements IPartitionManager
{
    private final UUID thisID;
    private ConsistentHashRing ring;
    
    public SimplePartitionManager(final String cacheName, final int numReplicas) {
        this.ring = new ConsistentHashRing(cacheName, numReplicas);
        this.thisID = HazelcastSingleton.getNodeId();
    }
    
    public SimplePartitionManager(final String cacheName, final int numReplicas, final int numPartitions) {
        this.ring = new ConsistentHashRing(cacheName, numReplicas, numPartitions);
        this.thisID = HazelcastSingleton.getNodeId();
    }
    
    @Override
    public void doShutdown() {
    }
    
    @Override
    public void setServers(final List<UUID> servers) {
        this.ring.set(servers);
    }
    
    @Override
    public int getNumReplicas() {
        return this.ring.getNumReplicas();
    }
    
    @Override
    public int getPartitionId(final Object key) {
        return this.ring.getPartitionId(key);
    }
    
    @Override
    public UUID getPartitionOwnerForKey(final Object key, final int n) {
        return this.ring.getUUIDForKey(key, n);
    }
    
    @Override
    public UUID getPartitionOwnerForPartition(final int partId, final int n) {
        return this.ring.getUUIDForPartition(partId, n);
    }
    
    @Override
    public UUID getFirstPartitionOwnerForPartition(final int partId) {
        return this.ring.getUUIDForPartition(partId, 0);
    }
    
    @Override
    public boolean isLocalPartitionByKey(final Object key, final int n) {
        return this.thisID.equals((Object)this.getPartitionOwnerForKey(key, n));
    }
    
    @Override
    public boolean isLocalPartitionById(final int partId, final int n) {
        return this.thisID.equals((Object)this.getPartitionOwnerForPartition(partId, n));
    }
    
    @Override
    public boolean hasLocalPartitionForId(final int partId) {
        if (this.ring.isFullyReplicated()) {
            return true;
        }
        for (int i = 0; i < this.ring.getNumReplicas(); ++i) {
            if (this.isLocalPartitionById(partId, i)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public boolean hasLocalPartitionForKey(final Object key) {
        final int partId = this.getPartitionId(key);
        return this.hasLocalPartitionForId(partId);
    }
    
    @Override
    public List<UUID> getAllReplicas(final int partId) {
        final List<UUID> ret = new ArrayList<UUID>();
        for (int i = 0; i < this.ring.getNumReplicas(); ++i) {
            final UUID uuid = this.getPartitionOwnerForPartition(partId, i);
            if (!ret.contains(uuid)) {
                ret.add(uuid);
            }
        }
        return ret;
    }
    
    @Override
    public boolean isLocalPartitionByUUID(final UUID uuid) {
        return this.thisID.equals((Object)uuid);
    }
    
    @Override
    public UUID getLocal() {
        return this.thisID;
    }
    
    @Override
    public boolean isMultiNode() {
        return this.ring.isMultiNode();
    }
    
    @Override
    public List<UUID> getPeers() {
        return new ArrayList<UUID>(this.ring.getNodes());
    }
    
    @Override
    public void addUpdateListener(final ConsistentHashRing.UpdateListener listener) {
        this.ring.addUpdateListener(listener);
    }
    
    @Override
    public int getNumberOfPartitions() {
        return this.ring.getNumberOfPartitions();
    }
}
