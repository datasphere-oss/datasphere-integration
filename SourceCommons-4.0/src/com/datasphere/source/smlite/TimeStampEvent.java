package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class TimeStampEvent extends SMEvent
{
    public TimeStampEvent() {
        super((short)9);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
