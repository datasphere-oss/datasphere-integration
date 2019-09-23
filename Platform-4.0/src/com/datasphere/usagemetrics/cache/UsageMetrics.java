package com.datasphere.usagemetrics.cache;

import java.io.*;
import org.joda.time.*;

public class UsageMetrics implements Serializable
{
    private static final long serialVersionUID = 5121903999710832879L;
    public long sourceBytes;
    public int dayCreated;
    
    public UsageMetrics() {
        this.dayCreated = getCurrentDay();
    }
    
    public void addMetrics(final com.datasphere.usagemetrics.persistence.jpa.UsageMetrics usageMetrics) {
        this.sourceBytes += usageMetrics.getSourceBytes();
    }
    
    private static int getCurrentDay() {
        final DateTime now = new DateTime();
        return now.dayOfYear().get();
    }
    
    public boolean isExpired() {
        return this.dayCreated != getCurrentDay();
    }
}
