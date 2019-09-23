package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;

public class HEntry
{
    final DARecord data;
    final long id;
    final long timestamp;
    
    HEntry(final DARecord data, final long id, final long timestamp) {
        this.data = data;
        this.id = id;
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "" + this.data + "<" + this.id + ">";
    }
}
