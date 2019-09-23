package com.datasphere.runtime.compiler.exprs;

import java.util.*;

public class FuncArgs
{
    public final List<ValueExpr> args;
    public final int options;
    
    public FuncArgs(final List<ValueExpr> args, final int options) {
        this.args = args;
        this.options = options;
    }
}
