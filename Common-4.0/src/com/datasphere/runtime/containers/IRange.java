package com.datasphere.runtime.containers;

import com.datasphere.runtime.*;

public interface IRange
{
    IBatch<DARecord> all();
    
    IBatch<DARecord> lookup(final int p0, final RecordKey p1);
    
    IRange update(final IRange p0);
    
    void beginTransaction();
    
    void endTransaction();
}
