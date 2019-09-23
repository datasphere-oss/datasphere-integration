package com.datasphere.cache;

import org.apache.log4j.*;

import com.datasphere.distribution.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;
import java.util.*;
import javax.cache.processor.*;

public class CacheStorage<K, V>
{
    private static Logger logger;
    private static final boolean DEBUG;
    public transient ConcurrentMap<K, V>[] partitions;
    private String cacheName;
    public Map<String, WAIndex<?, K, V>> indices;
    private long cacheHit;
    private long cacheMiss;
    
    public CacheStorage(final String cacheName, final int numPartitions) {
        this.indices = new HashMap<String, WAIndex<?, K, V>>();
        this.cacheHit = 0L;
        this.cacheMiss = 0L;
        this.cacheName = cacheName;
        this.partitions = (ConcurrentMap<K, V>[])new ConcurrentMap[numPartitions];
    }
    // 添加索引
    public <I> void addIndex(final String name, final WAIndex.Type type, final WAIndex.FieldAccessor<I, V> accessor, final IPartitionManager pm) {
        final WAIndex<I, K, V> index = new WAIndex<I, K, V>(type, accessor, pm);
        this.indices.put(name, index);
    }
    
    public <I> Map<K, V> getIndexedEqual(final String name, final I value) {
        final WAIndex<I, K, V> index = (WAIndex<I, K, V>)this.indices.get(name);
        if (index == null) {
            throw new IllegalArgumentException("No index with name " + name + " has been added for " + this.getName());
        }
        return index.getEqual(value);
    }
    // 获得索引范围
    public <I> Map<K, V> getIndexedRange(final String name, final I startVal, final I endVal) {
        final WAIndex<I, K, V> index = (WAIndex<I, K, V>)this.indices.get(name);
        if (index == null) {
            throw new IllegalArgumentException("No index with name " + name + " has been added for " + this.getName());
        }
        return index.getRange(startVal, endVal);
    }
    
    public V get(final int partId, final K key) {
        if (key == null) {
            return null;
        }
        V val = null;
        if (CacheStorage.DEBUG) {
            CacheStorage.logger.debug((Object)("GET K " + key));
        }
        if (this.partitions[partId] != null) {
            val = this.partitions[partId].get(key);
        }
        if (CacheStorage.DEBUG) {
            CacheStorage.logger.debug((Object)("Found " + key + " at partition : " + partId + "with value : " + val));
        }
        if (CacheStorage.logger.isInfoEnabled()) {
            if (val != null) {
                ++this.cacheHit;
            }
            else {
                ++this.cacheMiss;
            }
        }
        return val;
    }
    // 获得键
    public Set<K> getKeys(final int partId) {
        if (this.partitions[partId] != null) {
            return new HashSet<K>((Collection<? extends K>)this.partitions[partId].keySet());
        }
        return new HashSet<K>();
    }
    
    public Map<K, V> getAll(final int partId) {
        if (this.partitions[partId] != null) {
            return this.partitions[partId];
        }
        return new HashMap<K, V>();
    }
    
    public void putAll(final int partId, final Map<K, V> map) {
        if (this.partitions[partId] == null) {
            this.partitions[partId] = new ConcurrentHashMap<K, V>();
        }
        this.partitions[partId].putAll(map);
    }
    // 根据分区放入键值
    public void put(final int partId, final K key, final V value) {
        if (key == null || value == null) {
            if (CacheStorage.logger.isDebugEnabled()) {
                CacheStorage.logger.debug((Object)"Key or Value is Null!");
            }
        }
        else {
            if (CacheStorage.DEBUG) {
                CacheStorage.logger.debug((Object)("PUT " + partId + " key : " + key + " value : " + value));
            }
            if (this.partitions[partId] == null) {
                this.partitions[partId] = new ConcurrentHashMap<K, V>();
            }
            final ConcurrentMap<K, V> part = this.partitions[partId];
            if (part != null) {
                part.put(key, value);
                for (final Map.Entry<String, WAIndex<?, K, V>> indexEntry : this.indices.entrySet()) {
                    final WAIndex<?, K, V> index = indexEntry.getValue();
                    try {
                        index.add(partId, key, value);
                    }
                    catch (Throwable t) {
                        CacheStorage.logger.error((Object)("Problem adding to index " + indexEntry.getKey() + " for " + key + " = " + value + " on part " + partId), t);
                    }
                }
            }
        }
    }
    
    public V getAndPut(final int partId, final K k, final V v) {
        final V o = this.get(partId, k);
        this.put(partId, k, v);
        return o;
    }
    
    public boolean putIfAbsent(final int partId, final K key, final V value) {
        boolean result;
        if (key == null || value == null) {
            result = false;
        }
        else {
            final V current = this.get(partId, key);
            if (current == value) {
                result = false;
            }
            else {
                this.put(partId, key, value);
                result = true;
            }
        }
        return result;
    }
    // 是否包含键
    public boolean containsKey(final int partId, final K key) {
        boolean contains;
        if (key == null) {
            contains = false;
        }
        else {
            V val = null;
            if (CacheStorage.DEBUG) {
                CacheStorage.logger.debug((Object)("Searching for K " + key));
            }
            if (this.partitions[partId] != null) {
                val = this.partitions[partId].get(key);
            }
            if (CacheStorage.DEBUG) {
                CacheStorage.logger.debug((Object)("Found " + key + " at partition : " + partId + "with value : " + val));
            }
            contains = (val != null);
        }
        return contains;
    }
    // 删除键
    public boolean remove(final int partId, final K key) {
        boolean removed;
        if (key == null) {
            removed = false;
        }
        else if (this.partitions[partId] != null) {
            final V value = this.partitions[partId].remove(key);
            if (value == null) {
                removed = false;
            }
            else {
                removed = true;
                for (final WAIndex<?, K, V> index : this.indices.values()) {
                    index.remove(key, value);
                }
            }
        }
        else {
            removed = false;
        }
        return removed;
    }
    // 删除值
    public boolean remove(final int partId, final K key, final V value) {
        boolean removed;
        if (key == null) {
            removed = false;
        }
        else if (this.partitions[partId] != null) {
            removed = this.partitions[partId].remove(key, value);
            if (removed) {
                for (final WAIndex<?, K, V> index : this.indices.values()) {
                    index.remove(key, value);
                }
            }
        }
        else {
            removed = false;
        }
        return removed;
    }
    
    public V getAndRemove(final int partId, final K k) {
        final V oldVal = this.get(partId, k);
        this.remove(partId, k);
        return oldVal;
    }
    
    public boolean replace(final int partId, final K k, final V v, final V v2) {
        final V oldVal = this.get(partId, k);
        if (oldVal != null && oldVal.equals(v)) {
            this.put(partId, k, v2);
            return true;
        }
        return false;
    }
    
    public boolean replace(final int partId, final K k, final V v) {
        final V oldVal = this.get(partId, k);
        if (oldVal != null) {
            this.put(partId, k, v);
            return true;
        }
        return false;
    }
    
    public V getAndReplace(final int partId, final K k, final V v) {
        final V oldVal = this.get(partId, k);
        if (oldVal != null) {
            this.put(partId, k, v);
            return oldVal;
        }
        return null;
    }
    
    public void clear() {
        if (CacheStorage.logger.isInfoEnabled()) {
            CacheStorage.logger.info((Object)("Cache size for cache " + this.cacheName + " is " + this.cacheSize()));
            CacheStorage.logger.info((Object)("Cache hit for cache " + this.cacheName + " is " + this.cacheHit));
            CacheStorage.logger.info((Object)("Cache miss for cache " + this.cacheName + " is " + this.cacheMiss));
        }
        for (final Map<K, V> map : this.partitions) {
            if (map != null) {
                map.clear();
            }
        }
        for (final Map.Entry<String, WAIndex<?, K, V>> indexEntry : this.indices.entrySet()) {
            indexEntry.getValue().clear();
        }
    }
    
    public int cacheSize() {
        int cacheSize = 0;
        for (int i = 0; i < this.partitions.length; ++i) {
            cacheSize += this.size(i);
        }
        return cacheSize;
    }
    
    public int size(final int partId) {
        if (this.partitions[partId] != null) {
            return this.partitions[partId].size();
        }
        return 0;
    }
    
    public <T> T invoke(final int partId, final K k, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) throws EntryProcessorException {
        if (k == null) {
            return null;
        }
        final MutableEntry<K, V> me = (MutableEntry<K, V>)new MutableEntry<K, V>() {
            public K getKey() {
                return k;
            }
            
            public V getValue() {
                return CacheStorage.this.get(partId, k);
            }
            
            public boolean exists() {
                return CacheStorage.this.containsKey(partId, k);
            }
            
            public void remove() {
                CacheStorage.this.remove(partId, k);
            }
            
            public <T> T unwrap(final Class<T> clazz) {
                return null;
            }
            
            public void setValue(final V value) {
                CacheStorage.this.put(partId, k, value);
            }
        };
        return (T)kvtEntryProcessor.process((MutableEntry)me, objects);
    }
    
    public String getName() {
        return this.cacheName;
    }
    
    static {
        CacheStorage.logger = Logger.getLogger((Class)CacheStorage.class);
        DEBUG = CacheStorage.logger.isDebugEnabled();
    }
}
