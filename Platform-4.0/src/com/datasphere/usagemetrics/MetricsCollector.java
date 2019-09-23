package com.datasphere.usagemetrics;

import com.datasphere.usagemetrics.provider.*;
import com.datasphere.recovery.*;
import com.datasphere.uuid.*;
import com.datasphere.usagemetrics.metrics.*;

public class MetricsCollector implements SourceMetricsCollector
{
    private MetricsProvider metricsProvider;
    
    @Override
    public void flush() {
        if (this.metricsProvider != null) {
            this.metricsProvider.flush();
        }
    }
    
    @Override
    public void setSourcePosition(final SourcePosition sourcePosition, final UUID sourceID) {
        if (sourceID != null && sourcePosition != null) {
            final MetricsProvider metricsProvider = this.getMetricsProvider(sourceID);
            metricsProvider.setPosition(sourcePosition);
        }
    }
    
    @Override
    public void addSourceBytes(final long sourceBytes, final UUID sourceID) {
        if (sourceID != null && sourceBytes > 0L) {
            final MetricsProvider metricsProvider = this.getMetricsProvider(sourceID);
            metricsProvider.add(new Metric(MetricType.SOURCE_BYTES, sourceBytes));
        }
    }
    
    private MetricsProvider getMetricsProvider(final UUID sourceID) {
        if (this.metricsProvider == null) {
            this.metricsProvider = new MetricsProvider(sourceID);
        }
        return this.metricsProvider;
    }
}
