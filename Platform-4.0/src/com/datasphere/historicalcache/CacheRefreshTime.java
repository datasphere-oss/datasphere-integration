package com.datasphere.historicalcache;

import org.joda.time.*;

public class CacheRefreshTime
{
    private long refreshInterval;
    private LocalTime startTime;
    
    public CacheRefreshTime(final long refreshInterval, final LocalTime startTime) {
        this.refreshInterval = refreshInterval;
        this.startTime = startTime;
    }
    
    public boolean isSet() {
        return this.refreshInterval > 0L;
    }
    
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("");
        builder.append("StartTime: ").append(this.startTime).append(" Refresh Period: ").append(this.refreshInterval);
        return builder.toString();
    }
    
    public long getRefreshInterval() {
        return this.refreshInterval;
    }
    
    public LocalTime getStartTime() {
        return this.startTime;
    }
    
    public LocalTime subtract(final LocalTime time) {
        return this.startTime.minusHours(time.getHourOfDay()).minusMinutes(time.getMinuteOfHour()).minusSeconds(time.getSecondOfMinute());
    }
}
