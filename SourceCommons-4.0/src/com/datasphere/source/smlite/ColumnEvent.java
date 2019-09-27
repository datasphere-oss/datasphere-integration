package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class ColumnEvent extends SMEvent
{
    public ColumnEvent() {
        super((short)1);
        this.removePattern = true;
    }
    
    public ColumnEvent(final short st) {
        super(st);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
