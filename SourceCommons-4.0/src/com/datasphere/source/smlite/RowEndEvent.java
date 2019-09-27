package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class RowEndEvent extends SMEvent
{
    public RowEndEvent() {
        super((short)11);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
