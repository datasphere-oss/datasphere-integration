package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class QuoteBeginEvent extends SMEvent
{
    public QuoteBeginEvent() {
        super((short)4);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
