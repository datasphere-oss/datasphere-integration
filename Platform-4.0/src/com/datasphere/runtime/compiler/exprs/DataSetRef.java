package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public abstract class DataSetRef extends ValueExpr
{
    private int index;
    
    public DataSetRef(final ExprCmd kind, final int index) {
        super(kind);
        this.index = index;
    }
    
    public abstract DataSet getDataSet();
    
    public static DataSetRef makeDataSetRef(final DataSet dataSet) {
        return new DataSetRef(ExprCmd.DATASET, 0) {
            @Override
            public DataSet getDataSet() {
                return dataSet;
            }
        };
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitDataSetRef(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DataSetRef)) {
            return false;
        }
        final DataSetRef o = (DataSetRef)other;
        return super.equals(o) && this.getDataSet().equals(o.getDataSet()) && this.index == o.index;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).append(this.index).append((Object)this.getDataSet().getFullName()).toHashCode();
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.getDataSet().getName();
    }
    
    @Override
    public boolean isConst() {
        return false;
    }
    
    @Override
    public Class<?> getType() {
        return this.getDataSet().getJavaType();
    }
    
    @Override
    public FieldRef getField(final String fieldName) throws NoSuchFieldException, SecurityException {
        return this.getDataSet().makeFieldRef(this, fieldName);
    }
    
    public void setIndex(final int idx) {
        this.index = idx;
    }
    
    public int getIndex() {
        return this.index;
    }
}
