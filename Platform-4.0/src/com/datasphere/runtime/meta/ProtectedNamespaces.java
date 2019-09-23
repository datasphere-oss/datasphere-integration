package com.datasphere.runtime.meta;

public enum ProtectedNamespaces
{
    Global, 
    Admin;
    
    public static ProtectedNamespaces getEnum(final String value) {
        for (final ProtectedNamespaces v : values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        return null;
    }
}
