package com.datasphere.usagemetrics.metrics;

import java.util.concurrent.atomic.*;

public class Metric extends AtomicLong
{
    private static final long serialVersionUID = -8347106920574873582L;
    private final MetricType metricType;
    
    public Metric(final MetricType metricType, final long initialValue) {
        super(initialValue);
        throwIfNull(metricType);
        this.metricType = metricType;
    }
    
    public Metric(final MetricType metricType) {
        this(metricType, 0L);
    }
    
    public long add(final Metric other) {
        return this.doAtomicAdd(other.metricType, other.longValue());
    }
    
    public long sub(final Metric other) {
        return this.doAtomicAdd(other.metricType, -other.longValue());
    }
    
    private long doAtomicAdd(final MetricType metricType, final long value) {
        if (this.metricType != metricType) {
            throw new IllegalArgumentException();
        }
        return this.addAndGet(value);
    }
    
    private static void throwIfNull(final Object value) {
        if (value == null) {
            throw new IllegalArgumentException();
        }
    }
    
    public MetricType getType() {
        return this.metricType;
    }
    
    public int getTypeIndex() {
        return this.metricType.ordinal();
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        final Metric metric = (Metric)obj;
        return this.metricType == metric.metricType && this.longValue() == metric.longValue();
    }
    
    @Override
    public int hashCode() {
        final long value = this.longValue();
        int result = (int)(value ^ value >>> 32);
        result = 31 * result + this.metricType.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return "Metric{" + this.metricType + '=' + super.toString() + '}';
    }
}
