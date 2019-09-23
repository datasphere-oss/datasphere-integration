package com.datasphere.runtime;

import java.util.*;
import com.datasphere.runtime.compiler.*;

public class PropablePropertySet extends TypeDefOrName
{
    Map<String, Object> propertySet;
    
    public PropablePropertySet(final String typeName, final List<TypeField> typeDef) {
        super(typeName, typeDef);
    }
    
    public void setPropertySet(final Map<String, Object> prop) {
        this.propertySet = prop;
    }
    
    public Map<String, Object> getPropertySet() {
        return this.propertySet;
    }
}
