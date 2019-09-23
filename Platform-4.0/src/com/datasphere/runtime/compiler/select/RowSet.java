package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.*;
import org.apache.commons.lang.builder.*;
import java.util.*;
import com.datasphere.runtime.compiler.exprs.*;
import com.datasphere.runtime.utils.*;

public class RowSet
{
    private final Class<?> type;
    private final FieldAccessor fieldAccessor;
    private final RowAccessor rowAccessor;
    private final FieldFactory fieldFactory;
    private final List<Pair<String, Class<?>>> virtFields;
    
    public RowSet(final Class<?> type, final FieldAccessor fieldAccessor, final RowAccessor rowAccessor, final FieldFactory fieldFactory, final List<Pair<String, Class<?>>> virtFields) {
        this.type = type;
        this.fieldAccessor = fieldAccessor;
        this.rowAccessor = rowAccessor;
        this.fieldFactory = fieldFactory;
        this.virtFields = virtFields;
    }
    
    public final Class<?> getJavaType() {
        return this.type;
    }
    
    public final String getTypeName() {
        return this.type.getCanonicalName();
    }
    
    public final String rsToString() {
        final StringBuilder sb = new StringBuilder(this.getJavaType().getName());
        sb.append(" [\n");
        String sep = "";
        for (final Pair<String, Class<?>> p : this.getAllTypeFields()) {
            final String nameAndType = String.format("\t%-15s %s", p.first, p.second.getSimpleName());
            sb.append(sep + nameAndType);
            sep = ",\n";
        }
        sb.append("\n]");
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return "RowSet(" + this.rsToString() + ")";
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowSet)) {
            return false;
        }
        final RowSet other = (RowSet)o;
        return this.type == other.type && this.fieldFactory == other.fieldFactory && this.rowAccessor == other.rowAccessor && this.fieldAccessor == other.fieldAccessor;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.type.hashCode()).append(this.fieldFactory.hashCode()).append(this.rowAccessor.hashCode()).append(this.fieldAccessor.hashCode()).toHashCode();
    }
    
    public String genRowAccessor(final String obj) {
        return this.rowAccessor.genRowAccessor(obj);
    }
    
    public String genFieldAccessor(final String obj, final VirtFieldRef f) {
        return this.fieldAccessor.genFieldAccessor(obj, f);
    }
    
    public List<Pair<String, Class<?>>> getAllTypeFields() {
        final List<Pair<String, Class<?>>> ret = this.fieldFactory.getAllTypeFieldsImpl();
        if (this.virtFields != null) {
            ret.addAll(this.virtFields);
        }
        return ret;
    }
    
    public List<FieldRef> makeListOfAllFields(final ValueExpr expr) {
        return this.fieldFactory.makeListOfAllFields(expr, this.fieldAccessor);
    }
    
    public FieldRef makeFieldRef(final ValueExpr expr, final String fieldName) throws NoSuchFieldException {
        try {
            return this.fieldFactory.makeFieldRefImpl(expr, this.fieldAccessor, fieldName);
        }
        catch (NoSuchFieldException e) {
            if (this.virtFields != null) {
                for (final Pair<String, Class<?>> p : this.virtFields) {
                    final String fname = p.first;
                    if (NamePolicy.isEqual(fname, fieldName)) {
                        return new VirtFieldRef(expr, fname, p.second, -1, this.fieldAccessor);
                    }
                }
            }
            throw e;
        }
    }
}
