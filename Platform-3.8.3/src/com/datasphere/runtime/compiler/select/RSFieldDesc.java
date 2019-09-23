package com.datasphere.runtime.compiler.select;

import java.io.*;

public class RSFieldDesc implements Serializable
{
    private static final long serialVersionUID = -5535114504447103660L;
    public String name;
    public transient Class<?> type;
    
    public RSFieldDesc() {
    }
    
    public RSFieldDesc(final String name, final Class<?> type) {
        this.name = name;
        this.type = type;
    }
    
    @Override
    public String toString() {
        return "(" + this.name + ":" + ((this.type == null) ? "<NOTSET>" : this.type.getSimpleName()) + ")";
    }
}
