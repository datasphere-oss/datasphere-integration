package com.datasphere.usagemetrics.metrics;

import java.util.*;

public class Metrics
{
    private static final Set<MetricType> METRIC_TYPES;
    private static final int TYPE_COUNT;
    private final Metric[] metrics;
    
    public Metrics() {
        this.metrics = new Metric[Metrics.TYPE_COUNT];
        for (final MetricType type : Metrics.METRIC_TYPES) {
            this.metrics[type.ordinal()] = new Metric(type);
        }
    }
    
    public Metrics(final Metrics other) {
        this.metrics = new Metric[Metrics.TYPE_COUNT];
        for (final MetricType type : Metrics.METRIC_TYPES) {
            final Metric metric = other.getMetric(type);
            this.metrics[type.ordinal()] = new Metric(type, metric.longValue());
        }
    }
    
    public void add(final Metric metric) {
        final int index = metric.getTypeIndex();
        this.metrics[index].add(metric);
    }
    
    public void sub(final Metric metric) {
        final int index = metric.getTypeIndex();
        this.metrics[index].sub(metric);
    }
    
    public void add(final Metrics metrics) {
        if (metrics == null) {
            return;
        }
        for (final Metric metric : metrics.metrics) {
            this.add(metric);
        }
    }
    
    public void sub(final Metrics metrics) {
        if (metrics == null) {
            return;
        }
        for (final Metric metric : metrics.metrics) {
            this.sub(metric);
        }
    }
    
    public void clear() {
        for (final Metric metric : this.metrics) {
            this.clear(metric.getType());
        }
    }
    
    public void clear(final MetricType type) {
        final int index = type.ordinal();
        this.metrics[index].set(0L);
    }
    
    public long longValue(final MetricType type) {
        final int index = type.ordinal();
        return this.metrics[index].longValue();
    }
    
    public Metric getMetric(final MetricType type) {
        final int index = type.ordinal();
        return this.metrics[index];
    }
    
    public int size() {
        return this.metrics.length;
    }
    
    static {
        METRIC_TYPES = EnumSet.allOf(MetricType.class);
        TYPE_COUNT = Metrics.METRIC_TYPES.size();
    }
}
