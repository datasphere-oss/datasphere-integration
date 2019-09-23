package com.datasphere.runtime.compiler.visitors;

import com.datasphere.runtime.compiler.exprs.*;

public abstract class FindCommonSubexpression extends ExpressionVisitorDefaultImpl<Object>
{
    private final Rewriter rewriter;
    private boolean inAggFunc;
    
    public FindCommonSubexpression(final Rewriter r) {
        this.inAggFunc = false;
        this.rewriter = r;
    }
    
    Expr rewriteExpr(final Expr e) {
        return this.rewriter.rewriteExpr(e);
    }
    
    @Override
    public Expr visitExprDefault(final Expr e, final Object params) {
        e.visitArgs(this, (Object)null);
        return this.rewriteExpr(e);
    }
    
    public <T> T rewrite(final Expr e) {
        final T result = (T)this.visitExpr(e, null);
        return result;
    }
    
    @Override
    public Expr visitFuncCall(final FuncCall funcCall, final Object params) {
        if (funcCall.getAggrDesc() != null) {
            this.inAggFunc = true;
            funcCall.visitArgs(this, params);
            this.inAggFunc = false;
            final FuncCall fc = (FuncCall)this.rewriteExpr(funcCall);
            this.rewriter.addAggrExpr(fc);
            return fc;
        }
        return this.visitExprDefault(funcCall, params);
    }
    
    public boolean isInAggFunc() {
        return this.inAggFunc;
    }
    
    public interface Rewriter
    {
        Expr rewriteExpr(final Expr p0);
        
        void addAggrExpr(final FuncCall p0);
    }
}
