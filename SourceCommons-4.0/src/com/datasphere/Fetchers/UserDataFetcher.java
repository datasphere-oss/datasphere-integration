package com.datasphere.Fetchers;

import com.datasphere.proc.events.HDEvent;
import com.datasphere.utils.writers.common.EventDataObject;

public class UserDataFetcher extends Fetcher
{
    String userdataKey;
    
    public UserDataFetcher(final String key) {
        this.userdataKey = this.fetchArgument(key);
    }
    
    @Override
    public Object fetch(final HDEvent event) {
        return event.userdata.get(this.userdataKey);
    }
    
    @Override
    public Object fetch(final EventDataObject event) {
        return event.userdata.get(this.userdataKey);
    }
    
    @Override
    public String toString() {
        return "USERDATA(" + this.userdataKey + ")";
    }
}
