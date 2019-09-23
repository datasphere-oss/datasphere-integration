package com.datasphere.runtime.compiler.select;

import com.datasphere.uuid.*;

public abstract class TraslatedSchemaInfo
{
    public abstract UUID expectedTypeID();
    
    @Override
    public abstract String toString();
}
