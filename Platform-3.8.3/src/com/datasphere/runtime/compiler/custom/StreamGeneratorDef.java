package com.datasphere.runtime.compiler.custom;

import com.datasphere.uuid.*;

public class StreamGeneratorDef
{
    public final UUID outputTypeID;
    public final Class<?> outputType;
    public final Object[] args;
    public final String generatorClassName;
    
    public StreamGeneratorDef(final UUID outputTypeID, final Class<?> outputType, final Object[] args, final String generatorClassName) {
        this.outputTypeID = outputTypeID;
        this.outputType = outputType;
        this.args = args.clone();
        this.generatorClassName = generatorClassName;
    }
}
