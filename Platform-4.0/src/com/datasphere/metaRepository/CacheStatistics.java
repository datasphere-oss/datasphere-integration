package com.datasphere.metaRepository;

import java.util.*;

public class CacheStatistics
{
    protected long cachePuts;
    protected long cacheGets;
    protected long cacheHits;
    protected long cacheMisses;
    protected long cacheRemovals;
    protected long cacheEvictions;
    protected long totalGetMillis;
    protected long totalPutMillis;
    protected long totalRemovalsMillis;
    private Date lastRefreshDate;
    private final int HUNDRED = 100;
    
    public CacheStatistics() {
        this.lastRefreshDate = new Date();
    }
    
    public final Date getLastRefreshDate() {
        return this.lastRefreshDate;
    }
    
    public final void clear() {
        this.cachePuts = 0L;
        this.cacheGets = 0L;
        this.cacheHits = 0L;
        this.cacheMisses = 0L;
        this.cacheRemovals = 0L;
        this.cacheEvictions = 0L;
        this.totalGetMillis = 0L;
        this.totalPutMillis = 0L;
        this.totalRemovalsMillis = 0L;
        this.lastRefreshDate = new Date();
    }
    
    public final long getCachePuts() {
        return this.cachePuts;
    }
    
    public final long getCacheGets() {
        return this.cacheGets;
    }
    
    public final long getCacheHits() {
        return this.cacheHits;
    }
    
    public final long getCacheMisses() {
        return this.cacheMisses;
    }
    
    public final long getCacheRemovals() {
        return this.cacheRemovals;
    }
    
    public final long getCacheEvictions() {
        return this.cacheEvictions;
    }
    
    public final float getAverageGetMillis() {
        if (this.cacheGets == 0L) {
            return 0.0f;
        }
        return this.totalGetMillis / this.cacheGets;
    }
    
    public final float getAveragePutMillis() {
        if (this.cachePuts == 0L) {
            return 0.0f;
        }
        return this.totalPutMillis / this.cachePuts;
    }
    
    public final float getAverageRemoveMillis() {
        if (this.cacheRemovals == 0L) {
            return 0.0f;
        }
        return this.totalRemovalsMillis / this.cacheRemovals;
    }
    
    public final float getCacheHitPercentage() {
        if (this.cacheGets == 0L) {
            return 0.0f;
        }
        final long n = this.cacheHits / this.cacheGets;
        this.getClass();
        return n * 100L;
    }
    
    public final float getCacheMissPercentage() {
        if (this.cacheGets == 0L) {
            return 0.0f;
        }
        final long n = this.cacheMisses / this.cacheGets;
        this.getClass();
        return n * 100L;
    }
    
    public final long getTotalGetMillis() {
        return this.totalGetMillis;
    }
    
    public final long getTotalPutMillis() {
        return this.totalPutMillis;
    }
    
    public final long getTotalRemovalsMillis() {
        return this.totalRemovalsMillis;
    }
    
    public final void aggregate(final CacheStatistics statistics) {
        this.cachePuts += statistics.getCachePuts();
        this.cacheGets += statistics.getCacheGets();
        this.cacheHits += statistics.getCacheHits();
        this.cacheMisses += statistics.getCacheMisses();
        this.cacheRemovals += statistics.getCacheRemovals();
        this.cacheEvictions += statistics.getCacheEvictions();
        this.totalGetMillis += statistics.getTotalGetMillis();
        this.totalPutMillis += statistics.getTotalPutMillis();
        this.totalRemovalsMillis += statistics.getTotalRemovalsMillis();
    }
}
