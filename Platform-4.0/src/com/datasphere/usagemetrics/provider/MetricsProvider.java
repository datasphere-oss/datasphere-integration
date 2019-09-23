package com.datasphere.usagemetrics.provider;

import com.datasphere.uuid.*;
import com.datasphere.usagemetrics.collector.*;
import com.datasphere.recovery.*;
import com.datasphere.usagemetrics.metrics.*;

public class MetricsProvider implements Provider
{
    private final UUID sourceID;
    private final Metrics metrics;
    private final Collector collector;
    private SourcePosition sourcePosition;
    
    public MetricsProvider(final UUID sourceID) {
        this.metrics = new Metrics();
        this.sourceID = sourceID;
        this.collector = new Collector(this);
    }
    
    public void add(final Metric metric) {
        this.metrics.add(metric);
    }
    
    public void setPosition(final SourcePosition position) {
        this.sourcePosition = position;
    }
    
    public void flush() {
        this.collector.flush();
    }
    
    @Override
    public UUID getSourceID() {
        return this.sourceID;
    }
    
    @Override
    public SourcePosition getPosition() {
        return this.sourcePosition;
    }
    
    @Override
    public Metrics getMetrics() {
        return this.metrics;
    }
}
