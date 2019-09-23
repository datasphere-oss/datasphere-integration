package com.datasphere.runtime.compiler;

import java.io.*;

public class TypeName implements Serializable
{
    public String name;
    public int array_dimensions;
    
    public TypeName() {
    }
    
    public TypeName(final String name, final int array_dimensions) {
        this.name = name;
        this.array_dimensions = array_dimensions;
    }
    
    @Override
    public String toString() {
        return this.name + new String(new char[this.array_dimensions]).replace("\u0000", "[]");
    }
}
