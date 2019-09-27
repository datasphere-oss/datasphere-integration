package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class ResetEvent extends SMEvent
{
    public char[] buffer;
    
    public ResetEvent() {
        super((short)0);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
