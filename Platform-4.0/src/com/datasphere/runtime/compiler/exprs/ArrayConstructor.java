package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import java.util.*;

public class ArrayConstructor extends ConstructorCall
{
    public ArrayConstructor(final TypeName type, final List<ValueExpr> indexes) {
        super(type, indexes);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.objtype + "[" + this.argsToString() + "]";
    }
}
