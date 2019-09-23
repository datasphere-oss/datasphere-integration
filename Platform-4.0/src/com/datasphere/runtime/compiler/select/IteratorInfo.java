package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;

public interface IteratorInfo
{
    DataSet getParent();
    
    FieldRef getIterableField();
}
