package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class EscapeEvent extends SMEvent
{
    public EscapeEvent() {
        super((short)10);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
