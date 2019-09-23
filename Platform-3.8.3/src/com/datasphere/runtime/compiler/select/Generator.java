package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.meta.*;

public abstract class Generator
{
    public abstract CQExecutionPlan generate() throws Exception;
}
