package com.datasphere.hdstore;

import java.util.*;
import com.datasphere.runtime.*;

public enum Type
{
    IN_MEMORY, 
    STANDARD, 
    INTERVAL, 
    UNKNOWN;
    
    private static final String TYPE_NAME_PREFIX = "$Type";
    
    public String typeName() {
        return "$Type." + this.name();
    }
    
    public static Type getType(final Map<String, Object> properties, final Type defaultType) {
        if (properties != null) {
            for (final String propertyName : properties.keySet()) {
                final Type result = getTypeFromString(propertyName);
                if (result != null) {
                    return result;
                }
            }
        }
        return defaultType;
    }
    
    public static Type getType(final Iterable<Property> properties, final Type defaultType) {
        if (properties != null) {
            for (final Property property : properties) {
                final Type result = getTypeFromString(property.name);
                if (result != null) {
                    return result;
                }
            }
        }
        return defaultType;
    }
    
    public static Type getTypeFromString(final String string) {
        if (string.equalsIgnoreCase(Type.STANDARD.typeName())) {
            return Type.STANDARD;
        }
        if (string.equalsIgnoreCase(Type.IN_MEMORY.typeName())) {
            return Type.IN_MEMORY;
        }
        if (string.equalsIgnoreCase(Type.INTERVAL.typeName())) {
            return Type.INTERVAL;
        }
        return null;
    }
}
