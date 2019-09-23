package com.datasphere.runtime.compiler.exprs;

import org.apache.commons.lang.builder.*;
import java.lang.reflect.*;

public class ObjFieldRef extends FieldRef
{
    private final Field fieldRef;
    
    public ObjFieldRef(final ValueExpr expr, final String name, final Field field) {
        super(expr, name);
        this.fieldRef = field;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ObjFieldRef)) {
            return false;
        }
        final ObjFieldRef o = (ObjFieldRef)other;
        if (super.equals(o)) {
            if (this.fieldRef != null) {
                if (!this.fieldRef.equals(o.fieldRef)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).append((Object)((this.fieldRef == null) ? "length" : this.fieldRef.toString())).toHashCode();
    }
    
    @Override
    public Class<?> getType() {
        return (this.fieldRef == null) ? Integer.TYPE : this.fieldRef.getType();
    }
    
    @Override
    public boolean isStatic() {
        return this.fieldRef != null && Modifier.isStatic(this.fieldRef.getModifiers());
    }
    
    @Override
    public String genFieldAccess(final String obj) {
        return obj + "." + ((this.fieldRef == null) ? "length" : this.fieldRef.getName());
    }
}
