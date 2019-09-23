package com.datasphere.runtime.compiler.exprs;

import com.fasterxml.jackson.annotation.*;
import java.util.*;
import com.datasphere.runtime.compiler.visitors.*;
import com.datasphere.runtime.utils.*;
import java.lang.reflect.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({ @JsonSubTypes.Type(name = "Constant", value = Constant.class), @JsonSubTypes.Type(name = "Case", value = Case.class), @JsonSubTypes.Type(name = "CaseExpr", value = CaseExpr.class), @JsonSubTypes.Type(name = "DataSetRef", value = DataSetRef.class), @JsonSubTypes.Type(name = "ObjectRef", value = ObjectRef.class), @JsonSubTypes.Type(name = "TypeRef", value = TypeRef.class), @JsonSubTypes.Type(name = "ArrayTypeRef", value = ArrayTypeRef.class), @JsonSubTypes.Type(name = "ClassRef", value = ClassRef.class), @JsonSubTypes.Type(name = "StaticFieldRef", value = StaticFieldRef.class) })
public abstract class ValueExpr extends Expr
{
    public ValueExpr(final ExprCmd op) {
        super(op);
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        return Collections.emptyList();
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
    }
    
    public FieldRef getField(final String fieldName) throws NoSuchFieldException, SecurityException {
        final Class<?> type = this.getType();
        Field field = null;
        if (type.isArray() && fieldName.equalsIgnoreCase("length")) {
            field = null;
        }
        else {
            field = NamePolicy.getField(type, fieldName);
        }
        return new ObjFieldRef(this, fieldName, field);
    }
}
