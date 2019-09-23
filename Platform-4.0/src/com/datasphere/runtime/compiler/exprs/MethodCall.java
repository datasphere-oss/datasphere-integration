package com.datasphere.runtime.compiler.exprs;

import java.util.*;

public class MethodCall extends FuncCall
{
    public MethodCall(final String funcName, final List<ValueExpr> args, final int options) {
        super(funcName, args, options);
    }
    
    @Override
    public ValueExpr getThis() {
        return this.args.get(0);
    }
    
    @Override
    public List<ValueExpr> getFuncArgs() {
        return this.args.subList(1, this.args.size());
    }
    
    @Override
    public boolean isConst() {
        return !this.getFuncArgs().isEmpty() && super.isConst();
    }
}
