package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class QuoteEvent extends SMEvent
{
    public QuoteEvent() {
        super((short)3);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
