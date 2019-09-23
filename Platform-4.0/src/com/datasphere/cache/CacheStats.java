package com.datasphere.cache;

import javax.cache.management.*;
import java.io.*;
import javax.cache.*;
import java.util.concurrent.atomic.*;

public class CacheStats implements CacheStatisticsMXBean, Serializable
{
    private static final long serialVersionUID = 7350664589578546080L;
    private transient Cache<?, ?> cache;
    private final AtomicLong cacheRemovals;	// 缓存删除
    private final AtomicLong cacheExpiries;	// 缓存过期
    private final AtomicLong cachePuts;		// 缓存写入
    private final AtomicLong cacheHits;		// 缓存命中
    private final AtomicLong cacheMisses;	// 缓存丢失
    private final AtomicLong localHits;
    private final AtomicLong localMisses;
    private final AtomicLong remoteHits;
    private final AtomicLong remoteMisses;
    private final AtomicLong cacheEvictions;
    private final AtomicLong totalPutNanos;
    private final AtomicLong totalGetNanos;
    private final AtomicLong totalRemovalNanos;
    private final float HUNDRED = 100.0f;
    
    public CacheStats(final Cache<?, ?> that) {
        this.cacheRemovals = new AtomicLong(0L);
        this.cacheExpiries = new AtomicLong(0L);
        this.cachePuts = new AtomicLong(0L);
        this.cacheHits = new AtomicLong(0L);
        this.cacheMisses = new AtomicLong(0L);
        this.localHits = new AtomicLong(0L);
        this.localMisses = new AtomicLong(0L);
        this.remoteHits = new AtomicLong(0L);
        this.remoteMisses = new AtomicLong(0L);
        this.cacheEvictions = new AtomicLong(0L);
        this.totalPutNanos = new AtomicLong(0L);
        this.totalGetNanos = new AtomicLong(0L);
        this.totalRemovalNanos = new AtomicLong(0L);
        this.cache = that;
    }
    
    public void clear() {
        this.cachePuts.set(0L);
        this.cacheMisses.set(0L);
        this.cacheRemovals.set(0L);
        this.cacheExpiries.set(0L);
        this.cacheHits.set(0L);
        this.cacheEvictions.set(0L);
        this.totalPutNanos.set(0L);
        this.totalGetNanos.set(0L);
        this.totalRemovalNanos.set(0L);
    }
    
    public long getCacheHits() {
        return this.cacheHits.longValue();
    }
    
    public float getCacheHitPercentage() {
        if (this.getCacheHits() == 0L) {
            return 0.0f;
        }
        return this.getCacheHits() / this.getCacheGets() * 100.0f;
    }
    
    public long getCacheMisses() {
        return this.cacheMisses.longValue();
    }
    
    public long getLocalHits() {
        return this.localHits.longValue();
    }
    
    public long getLocalMisses() {
        return this.localMisses.longValue();
    }
    
    public long getRemoteHits() {
        return this.remoteHits.longValue();
    }
    
    public long getRemoteMisses() {
        return this.remoteMisses.longValue();
    }
    
    public float getCacheMissPercentage() {
        if (this.getCacheMisses() == 0L) {
            return 0.0f;
        }
        return this.getCacheMisses() / this.getCacheGets() * 100.0f;
    }
    
    public long getCacheGets() {
        return this.getCacheHits() + this.getCacheMisses();
    }
    
    public long getCachePuts() {
        return this.cachePuts.longValue();
    }
    
    public long getCacheRemovals() {
        return this.cacheRemovals.longValue();
    }
    
    public long getCacheEvictions() {
        return this.cacheEvictions.longValue();
    }
    
    public float getAverageGetTime() {
        if (this.getCacheGets() == 0L) {
            return 0.0f;
        }
        return this.getCacheGets() / this.totalGetNanos.longValue() * 100.0f;
    }
    
    public float getAveragePutTime() {
        if (this.getCachePuts() == 0L) {
            return 0.0f;
        }
        return this.getCachePuts() / this.totalPutNanos.longValue() * 100.0f;
    }
    
    public float getAverageRemoveTime() {
        if (this.getCacheRemovals() == 0L) {
            return 0.0f;
        }
        return this.getCacheRemovals() / this.totalRemovalNanos.longValue() * 100.0f;
    }
    
    public void incrementRemovals() {
        this.cacheRemovals.getAndIncrement();
    }
    
    public void incrementExpiries() {
        this.cacheExpiries.getAndIncrement();
    }
    
    public void incrementPuts() {
        this.cachePuts.getAndIncrement();
    }
    
    public void incrementHits() {
        this.cacheHits.getAndIncrement();
    }
    
    public void incrementMisses() {
        this.cacheMisses.getAndIncrement();
    }
    
    public void incrementLocalHits() {
        this.localHits.getAndIncrement();
    }
    
    public void incrementLocalMisses() {
        this.localMisses.getAndIncrement();
    }
    
    public void incrementRemoteHits() {
        this.remoteHits.getAndIncrement();
    }
    
    public void incrementRemoteMisses() {
        this.remoteMisses.getAndIncrement();
    }
    
    public void incrementEvictions() {
        this.cacheEvictions.getAndIncrement();
    }
    
    public void addGetTimeNanos(final long duration) {
        if (this.totalGetNanos.get() <= Long.MAX_VALUE - duration) {
            this.totalGetNanos.addAndGet(duration);
        }
        else {
            this.clear();
            this.totalGetNanos.set(duration);
        }
    }
    
    public void addPutTimeNano(final long duration) {
        if (this.totalPutNanos.get() <= Long.MAX_VALUE - duration) {
            this.totalPutNanos.addAndGet(duration);
        }
        else {
            this.clear();
            this.totalPutNanos.set(duration);
        }
    }
    
    public void addRemoveTimeNano(final long duration) {
        if (this.totalRemovalNanos.get() <= Long.MAX_VALUE - duration) {
            this.totalRemovalNanos.addAndGet(duration);
        }
        else {
            this.clear();
            this.totalRemovalNanos.set(duration);
        }
    }
}
