package com.datasphere.runtime.compiler.select;

import java.util.*;

public abstract class IndexInfo
{
    public abstract List<Integer> getListOfIndexesForFields(final Set<String> p0);
    
    public abstract List<String> indexFieldList(final int p0);
    
    @Override
    public abstract String toString();
}
