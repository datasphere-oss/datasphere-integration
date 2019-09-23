package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import com.fasterxml.jackson.databind.*;

public class IndexExpr extends Operation
{
    public IndexExpr(final ValueExpr index, final ValueExpr expr) {
        super(ExprCmd.INDEX, AST.NewList(expr, index));
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
    
    public ValueExpr getIndex() {
        return this.args.get(1);
    }
    
    public void setIndex(final ValueExpr index) {
        this.args.set(1, index);
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitIndexExpr(this, params);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.getExpr() + "[" + this.getIndex() + "]";
    }
    
    private Class<?> getBaseType() {
        return this.getExpr().getType();
    }
    
    @Override
    public Class<?> getType() {
        if (this.baseIsJsonNode()) {
            return JsonNode.class;
        }
        final Class<?> ta = this.getBaseType();
        final Class<?> t = (ta == null) ? null : ta.getComponentType();
        return t;
    }
    
    public boolean baseIsJsonNode() {
        final Class<?> t = this.getBaseType();
        return JsonNode.class.isAssignableFrom(t);
    }
    
    public boolean baseIsArray() {
        final Class<?> t = this.getBaseType();
        return t.isArray() || JsonNode.class.isAssignableFrom(t);
    }
}
