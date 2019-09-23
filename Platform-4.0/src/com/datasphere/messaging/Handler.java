package com.datasphere.messaging;

import com.datasphere.runtime.components.*;

public interface Handler
{
    void onMessage(final Object p0);
    
    String getName();
    
    FlowComponent getOwner();
}
