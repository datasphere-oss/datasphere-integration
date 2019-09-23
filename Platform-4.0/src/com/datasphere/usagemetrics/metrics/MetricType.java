package com.datasphere.usagemetrics.metrics;

import java.util.*;

public enum MetricType
{
    SOURCE_BYTES;
    
    public static int maxIndex() {
        return count() - 1;
    }
    
    public static int count() {
        final Set<MetricType> allValues = EnumSet.allOf(MetricType.class);
        return allValues.size();
    }
    
    @Override
    public String toString() {
        return this.name();
    }
}
