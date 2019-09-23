package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;

public abstract class FieldAccessor
{
    public abstract String genFieldAccessor(final String p0, final VirtFieldRef p1);
    
    @Override
    public abstract String toString();
}
