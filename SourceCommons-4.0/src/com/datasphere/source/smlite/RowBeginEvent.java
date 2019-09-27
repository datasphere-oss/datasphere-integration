package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class RowBeginEvent extends SMEvent
{
    public RowBeginEvent() {
        super((short)8);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
