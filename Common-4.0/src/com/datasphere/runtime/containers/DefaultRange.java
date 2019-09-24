package com.datasphere.runtime.containers;

import com.datasphere.runtime.*;

public class DefaultRange implements IRange
{
    public static final IRange EMPTY_RANGE;
    
    @Override
    public IBatch all() {
        return DefaultBatch.EMPTY_BATCH;
    }
    
    @Override
    public IBatch lookup(final int indexID, final RecordKey key) {
        return DefaultBatch.EMPTY_BATCH;
    }
    
    @Override
    public IRange update(final IRange r) {
        return r;
    }
    
    @Override
    public void beginTransaction() {
    }
    
    @Override
    public void endTransaction() {
    }
    
    static {
        EMPTY_RANGE = new DefaultRange();
    }
}
