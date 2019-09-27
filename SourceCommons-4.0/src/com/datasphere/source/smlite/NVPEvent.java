package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class NVPEvent extends RowEvent
{
    public NVPEvent(final char[] data) {
        super(data);
    }
    
    public NVPEvent() {
    }
    
    @Override
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
