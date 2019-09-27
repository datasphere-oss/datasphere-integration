package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class EndOfBlockEvent extends SMEvent
{
    public EndOfBlockEvent() {
        super((short)7);
        this.length = 0;
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
