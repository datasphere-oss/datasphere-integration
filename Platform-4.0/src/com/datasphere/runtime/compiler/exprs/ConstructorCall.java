package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import java.lang.reflect.*;
import java.util.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public class ConstructorCall extends Operation
{
    public final TypeName objtype;
    private Constructor<?> constructor;
    
    public ConstructorCall(final TypeName type, final List<ValueExpr> args) {
        super(ExprCmd.CONSTRUCTOR, args);
        this.objtype = type;
        this.constructor = null;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.objtype + "(" + this.argsToString() + ")";
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitConstructorCall(this, params);
    }
    
    public void setConstructor(final Constructor<?> con) {
        this.constructor = con;
    }
    
    public Constructor<?> getConstructor() {
        return this.constructor;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ConstructorCall)) {
            return false;
        }
        final ConstructorCall o = (ConstructorCall)other;
        return super.equals(o) && this.objtype.equals(o.objtype) && this.constructor.equals(o.constructor);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.toString()).toHashCode();
    }
    
    @Override
    public Class<?> getType() {
        return this.constructor.getDeclaringClass();
    }
}
