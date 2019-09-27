package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class QuoteEndEvent extends SMEvent
{
    public QuoteEndEvent() {
        super((short)5);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
