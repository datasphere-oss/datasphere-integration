package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.utils.*;
import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public abstract class Operation extends ValueExpr
{
    public final List<ValueExpr> args;
    
    public Operation(final ExprCmd op, final List<ValueExpr> args) {
        super(op);
        assert args != null;
        this.args = args;
    }
    
    protected String argsToString() {
        return StringUtils.join((List)this.args);
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        return this.args;
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
        final ListIterator<ValueExpr> it = this.args.listIterator();
        while (it.hasNext()) {
            final ValueExpr e = it.next();
            final ValueExpr enew = (ValueExpr)v.visitExpr(e, params);
            if (enew != null) {
                it.set(enew);
            }
        }
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.args;
    }
}
