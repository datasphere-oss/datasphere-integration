package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;

public class RowEvent extends SMEvent
{
    char[] buffer;
    int rowBegin;
    
    public RowEvent(final char[] array) {
        super((short)2);
        this.buffer = array;
    }
    
    public RowEvent() {
        super((short)2);
        this.removePattern = true;
    }
    
    public void rowBegin(final int offset) {
        this.rowBegin = offset;
    }
    
    public int rowBegin() {
        return this.rowBegin;
    }
    
    public char[] array() {
        return this.buffer;
    }
    
    public void array(final char[] a) {
        this.buffer = a;
    }
    
    public void publishEvent(final SMCallback callback) {
        callback.onEvent(this);
    }
}
