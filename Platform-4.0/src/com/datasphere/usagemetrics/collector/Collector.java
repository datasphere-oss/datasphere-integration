package com.datasphere.usagemetrics.collector;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.CustomThreadFactory;
import com.datasphere.runtime.Server;
import com.datasphere.usagemetrics.Constants;
import com.datasphere.usagemetrics.cache.Cache;
import com.datasphere.usagemetrics.metrics.MetricType;
import com.datasphere.usagemetrics.metrics.Metrics;
import com.datasphere.usagemetrics.persistence.EntityStore;
import com.datasphere.usagemetrics.persistence.jpa.UsageMetrics;
import com.datasphere.usagemetrics.provider.Provider;

public class Collector implements Runnable
{
    private static final Logger logger;
    private final CollectorType collectorType;
    private final ProviderReference providerReference;
    private final ScheduledFuture<?> task;
    private final UsageMetrics usageMetrics;
    private final Metrics usageMetricsToPersist;
    private long lastPersisted;
    private static final ScheduledExecutorService executor;
    
    public Collector(final Provider provider) {
        this(provider, Server.persistenceIsEnabled() ? CollectorType.PERSIST_AND_CACHE : CollectorType.CACHE_ONLY);
    }
    
    public Collector(final Provider provider, final CollectorType collectorType) {
        this.usageMetrics = new UsageMetrics();
        this.usageMetricsToPersist = new Metrics();
        this.lastPersisted = 0L;
        if (provider == null) {
            throw new IllegalArgumentException();
        }
        this.collectorType = collectorType;
        this.providerReference = new ProviderReference(provider);
        this.task = Collector.executor.scheduleWithFixedDelay(this, 2000L, 2000L, TimeUnit.MILLISECONDS);
        this.collectIdentifiers();
    }
    
    private void collectIdentifiers() {
        final Provider provider = this.getProvider();
        this.usageMetrics.setSourceId(provider.getSourceID().toString());
        this.usageMetrics.setPartitionId(Constants.PARTITION_ID.toString());
    }
    
    protected Provider getProvider() {
        return this.providerReference.get();
    }
    
    public void flush() {
        this.lastPersisted = 0L;
        this.run();
    }
    
    @Override
    public void run() {
        final Provider provider = this.getProvider();
        if (provider == null) {
            this.cancel();
            return;
        }
        if (this.collectUsageMetrics()) {
            this.cacheMetrics(this.usageMetrics);
            resetMetrics(this.usageMetrics);
        }
        if (this.canPersistMetrics() && this.persistUsageMetrics()) {
            this.usageMetricsToPersist.clear();
        }
    }
    
    public void cancel() {
        this.task.cancel(true);
    }
    
    private boolean collectUsageMetrics() {
        final boolean metricsChanged = this.collectMetrics();
        if (metricsChanged) {
            this.collectPosition();
            this.collectRecorded();
        }
        return metricsChanged;
    }
    
    private boolean collectMetrics() {
        final Metrics metrics = this.collectProviderMetrics();
        final boolean changed = metricsContainData(metrics);
        if (changed) {
            this.collectMetricsValues(this.usageMetrics, metrics);
            this.usageMetricsToPersist.add(metrics);
        }
        return changed;
    }
    
    private void collectMetricsValues(final UsageMetrics usageMetrics, final Metrics metrics) {
        usageMetrics.addSourceBytes(metrics.longValue(MetricType.SOURCE_BYTES));
    }
    
    private Metrics collectProviderMetrics() {
        final Provider provider = this.getProvider();
        final Metrics providerMetrics = provider.getMetrics();
        final Metrics collectedMetrics = new Metrics(providerMetrics);
        providerMetrics.sub(collectedMetrics);
        return collectedMetrics;
    }
    
    private void collectPosition() {
        final Provider provider = this.getProvider();
        final SourcePosition position = provider.getPosition();
        this.usageMetrics.setPosition(SerializableToByteArray.getBytes((Serializable)position));
    }
    
    private void collectRecorded() {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        this.usageMetrics.setRecorded(now);
    }
    
    private static void collectChecksum(final UsageMetrics usageMetrics) {
        final Checksum checker = new Checksum(usageMetrics);
        final String checksum = checker.toString();
        usageMetrics.setChecksum(checksum);
    }
    
    private boolean canPersistMetrics() {
        boolean result = false;
        if (this.collectorType != CollectorType.CACHE_ONLY && metricsContainData(this.usageMetricsToPersist)) {
            final long now = System.currentTimeMillis();
            final long elapsed = now - this.lastPersisted;
            result = (elapsed >= 60000L);
        }
        return result;
    }
    
    private boolean persistUsageMetrics() {
        final UsageMetrics toPersist = new UsageMetrics();
        toPersist.setSourceId(this.usageMetrics.getSourceId());
        toPersist.setPartitionId(this.usageMetrics.getPartitionId());
        toPersist.setRecorded(this.usageMetrics.getRecorded());
        toPersist.setPosition(this.usageMetrics.getPosition());
        toPersist.setSourceBytes(this.usageMetricsToPersist.longValue(MetricType.SOURCE_BYTES));
        collectChecksum(toPersist);
        final EntityStore entityStore = new EntityStore();
        this.lastPersisted = System.currentTimeMillis();
        return entityStore.persistRow(toPersist);
    }
    
    private static boolean metricsContainData(final Metrics metrics) {
        return metrics.longValue(MetricType.SOURCE_BYTES) != 0L;
    }
    
    private void cacheMetrics(final UsageMetrics usageMetrics) {
        if (this.collectorType != CollectorType.PERSIST_ONLY) {
            Cache.addUsageMetrics(usageMetrics);
        }
    }
    
    private static void resetMetrics(final UsageMetrics usageMetrics) {
        usageMetrics.setSourceBytes(0L);
    }
    
    static {
        logger = Logger.getLogger((Class)Collector.class);
        executor = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("Collector_WorkerThread", true));
    }
    
    private static class ProviderReference extends WeakReference<Provider>
    {
        private ProviderReference(final Provider instance) {
            super(instance);
        }
    }
}
