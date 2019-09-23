package com.datasphere.runtime.window;

class TimeIndexEntry
{
    final long createdTimestamp;
    int count;
    
    TimeIndexEntry(final int count, final long now) {
        this.count = count;
        this.createdTimestamp = now;
    }
}
