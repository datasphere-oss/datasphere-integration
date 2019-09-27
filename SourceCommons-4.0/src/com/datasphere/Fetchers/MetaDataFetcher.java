package com.datasphere.Fetchers;

import com.datasphere.proc.events.*;
import com.datasphere.utils.writers.common.*;

public class MetaDataFetcher extends Fetcher
{
    String metaKey;
    
    public MetaDataFetcher(final String key) {
        this.metaKey = this.fetchArgument(key);
    }
    
    @Override
    public Object fetch(final HDEvent event) {
        return event.metadata.get(this.metaKey);
    }
    
    @Override
    public Object fetch(final EventDataObject event) {
        return event.metadata.get(this.metaKey);
    }
    
    @Override
    public String toString() {
        return "META(" + this.metaKey + ")";
    }
}
