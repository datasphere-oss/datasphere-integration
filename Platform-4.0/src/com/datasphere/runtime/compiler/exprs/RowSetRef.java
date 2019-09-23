package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public class RowSetRef extends Operation
{
    private final RowSet rowSet;
    
    public RowSetRef(final ValueExpr expr, final RowSet rs) {
        super(ExprCmd.ROWSET, AST.NewList(expr));
        this.rowSet = rs;
    }
    
    @Override
    public Class<?> getType() {
        return this.rowSet.getJavaType();
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitRowSetRef(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RowSetRef)) {
            return false;
        }
        final RowSetRef o = (RowSetRef)other;
        return super.equals(o) && this.rowSet.equals(o.rowSet);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.rowSet.toString()).append(super.hashCode()).toHashCode();
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.rowSet.toString();
    }
    
    @Override
    public FieldRef getField(final String fieldName) throws NoSuchFieldException, SecurityException {
        return this.rowSet.makeFieldRef(this, fieldName);
    }
    
    public RowSet getRowSet() {
        return this.rowSet;
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
}
