package com.datasphere.source.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.intf.*;

public abstract class SMState implements State
{
    public abstract Constant.status canAccept(final char p0);
    
    public abstract void reset();
    
    public abstract String getCharsOfInterest();
}
