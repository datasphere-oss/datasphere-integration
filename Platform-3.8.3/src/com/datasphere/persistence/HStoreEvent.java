package com.datasphere.persistence;

import com.datasphere.hd.*;

public class HStoreEvent
{
    private final HD value;
    
    public HStoreEvent(final HD value) {
        this.value = value;
    }
    
    public HD getValue() {
        return this.value;
    }
}
