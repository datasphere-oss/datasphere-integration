package com.datasphere.runtime.compiler.stmts;

import java.util.*;

public class EventType
{
    public final String typeName;
    public final List<String> keyFields;
    
    public EventType(final String typeName, final List<String> keyFields) {
        this.typeName = typeName;
        this.keyFields = keyFields;
    }
    
    @Override
    public String toString() {
        return this.typeName + " " + this.keyFields;
    }
}
