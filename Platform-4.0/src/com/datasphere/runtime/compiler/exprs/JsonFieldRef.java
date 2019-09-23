package com.datasphere.runtime.compiler.exprs;

import com.fasterxml.jackson.databind.*;

public class JsonFieldRef extends FieldRef
{
    public JsonFieldRef(final ValueExpr expr, final String name) {
        super(expr, name);
    }
    
    @Override
    public Class<?> getType() {
        return JsonNode.class;
    }
    
    @Override
    public String genFieldAccess(final String obj) {
        return obj + ".get(\"" + this.getName() + "\")";
    }
}
