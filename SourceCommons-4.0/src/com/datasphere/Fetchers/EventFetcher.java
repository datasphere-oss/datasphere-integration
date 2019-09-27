package com.datasphere.Fetchers;

import com.datasphere.proc.events.*;
import com.datasphere.utils.writers.common.*;

public class EventFetcher extends Fetcher
{
    int colIndex;
    
    public EventFetcher(final int index) {
        this.colIndex = index;
    }
    
    @Override
    public Object fetch(final EventDataObject event) {
        return event.data[this.colIndex];
    }
    
    @Override
    public Object fetch(final EventDataObject event, final boolean before) {
        return event.before[this.colIndex];
    }
    
    @Override
    public Object fetch(final HDEvent event) {
        return event.data[this.colIndex];
    }
    
    @Override
    public Object fetch(final HDEvent event, final boolean before) {
        return event.before[this.colIndex];
    }
}
