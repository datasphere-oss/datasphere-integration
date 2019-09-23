package com.datasphere.runtime.compiler;

import java.io.*;

public class TypeField implements Serializable
{
    public String fieldName;
    public TypeName fieldType;
    public boolean isPartOfKey;
    
    public TypeField() {
    }
    
    public TypeField(final String name, final TypeName type, final boolean iskey) {
        this.fieldName = name;
        this.fieldType = type;
        this.isPartOfKey = iskey;
    }
    
    @Override
    public String toString() {
        return this.fieldName + " " + this.fieldType + (this.isPartOfKey ? " KEY" : "");
    }
}
