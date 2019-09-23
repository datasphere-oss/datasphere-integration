package com.datasphere.cache;

import javax.cache.*;

import java.io.*;

import com.datasphere.distribution.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.util.*;

public interface ICache<K, V> extends Cache<K, V>, Queryable<K, V>
{
    List<V> getAllData();
    
    Set<K> getAllKeys();
    
    long size();
    
    boolean statisticsEnabled();
    
     <I> void addIndex(final String p0, final WAIndex.Type p1, final WAIndex.FieldAccessor<I, V> p2);
    
    Iterator<Cache.Entry<K, V>> iterator(final Filter<K, V> p0);
    
    Set<CacheInfo> getCacheInfo();
    
    CacheStats getStats();
    
    public static class PartInfo implements Serializable
    {
        private static final long serialVersionUID = -2088307372153177651L;
        public int partID;
        public int size;
        public String replicaNumber;
        
        public PartInfo() {
        }
        
        public PartInfo(final int partID, final int size, final int replicaNumber) {
            this.partID = partID;
            this.size = size;
            this.replicaNumber = ((replicaNumber == -1) ? "stale" : ("" + replicaNumber));
        }
        
        @Override
        public String toString() {
            return "PART[" + this.partID + "][" + this.replicaNumber + "]=" + this.size;
        }
    }
    
    public static class CacheInfo implements Serializable
    {
        private static final long serialVersionUID = 1328368413474470229L;
        public String cacheName;
        public UUID serverID;
        public int replicationFactor;
        public int totalSize;
        public int totalStaleSize;
        public int numParts;
        public int numStaleParts;
        public int[] replicaSizes;
        public int[] replicaParts;
        public List<PartInfo> parts;
        public Map<String, Integer> indices;
        
        public CacheInfo() {
            this.totalSize = 0;
            this.totalStaleSize = 0;
            this.numParts = 0;
            this.numStaleParts = 0;
            this.replicaSizes = null;
            this.replicaParts = null;
            this.parts = new ArrayList<PartInfo>();
            this.indices = new HashMap<String, Integer>();
        }
    }
}
