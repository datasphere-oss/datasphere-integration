package com.datasphere.runtime.compiler.select;

import java.util.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.compiler.exprs.*;

public abstract class FieldFactory
{
    public abstract List<Pair<String, Class<?>>> getAllTypeFieldsImpl();
    
    public abstract List<FieldRef> makeListOfAllFields(final ValueExpr p0, final FieldAccessor p1);
    
    public abstract FieldRef makeFieldRefImpl(final ValueExpr p0, final FieldAccessor p1, final String p2) throws NoSuchFieldException, SecurityException;
    
    @Override
    public abstract String toString();
}
