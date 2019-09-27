package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class CommentEvent extends SMEvent
{
    public CommentEvent() {
        super((short)6);
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
