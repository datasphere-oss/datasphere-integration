package com.datasphere.runtime.compiler;

import org.apache.commons.lang.builder.*;

public class ObjectName
{
    String namespace;
    String name;
    
    private ObjectName(final String namespace, final String name) {
        this.namespace = namespace;
        this.name = name;
    }
    
    public static ObjectName makeObjectName(final String namespace, final String name) {
        return new ObjectName(namespace, name);
    }
    
    public String getFullName() {
        return this.getNamespace() + "." + this.getName();
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getNamespace() {
        return this.namespace;
    }
    
    @Override
    public String toString() {
        return " Namespace : " + this.namespace + "Name : " + this.name;
    }
    
    @Override
    public boolean equals(final Object obj) {
        return this.namespace == ((ObjectName)obj).namespace && this.name == ((ObjectName)obj).name;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.namespace).append((Object)this.name).toHashCode();
    }
}
