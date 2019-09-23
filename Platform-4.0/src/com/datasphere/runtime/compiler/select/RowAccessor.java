package com.datasphere.runtime.compiler.select;

public abstract class RowAccessor
{
    public abstract String genRowAccessor(final String p0);
    
    @Override
    public abstract String toString();
}
