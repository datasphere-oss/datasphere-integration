package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.*;
import org.apache.commons.lang.builder.*;

public class VirtFieldRef extends FieldRef
{
    private final Class<?> type;
    private final int index;
    private final FieldAccessor fieldAccessor;
    
    public VirtFieldRef(final ValueExpr expr, final String name, final Class<?> type, final int index, final FieldAccessor fieldAccessor) {
        super(expr, name);
        this.type = (type.isPrimitive() ? CompilerUtils.getBoxingType(type) : type);
        this.index = index;
        this.fieldAccessor = fieldAccessor;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof VirtFieldRef)) {
            return false;
        }
        final VirtFieldRef o = (VirtFieldRef)other;
        return super.equals(o) && this.type.equals(o.type) && this.index == o.index;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).append(this.index).append((Object)this.type).toHashCode();
    }
    
    @Override
    public Class<?> getType() {
        return this.type;
    }
    
    public int getIndex() {
        return this.index;
    }
    
    @Override
    public String genFieldAccess(final String obj) {
        assert !this.type.isPrimitive();
        final String fld = this.fieldAccessor.genFieldAccessor(obj, this);
        return "((" + this.getTypeName() + ")" + fld + ")";
    }
}
