package com.datasphere.runtime.compiler;

import java.util.*;

public class TypeDefOrName
{
    public final String typeName;
    public final List<TypeField> typeDef;
    public String extendsTypeName;
    
    public TypeDefOrName(final String typeName, final List<TypeField> typeDef) {
        this.extendsTypeName = null;
        this.typeName = typeName;
        this.typeDef = typeDef;
    }
    
    public void setExtendsTypeName(final String extendsTypeName) {
        this.extendsTypeName = extendsTypeName;
    }
    
    @Override
    public String toString() {
        return "typedef " + this.typeName + " " + this.typeDef + ((this.extendsTypeName != null) ? (" extends " + this.extendsTypeName) : "");
    }
}
