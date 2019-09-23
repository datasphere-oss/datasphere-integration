package com.datasphere.runtime;

import java.io.*;
import org.apache.commons.lang.builder.*;

public class Property implements Serializable
{
    private static final long serialVersionUID = 6042801189452241946L;
    public final String name;
    public final Object value;
    
    public Property() {
        this.name = null;
        this.value = null;
    }
    
    public Property(final String nameValue) {
        final int colonPos = nameValue.indexOf(58);
        if (colonPos == -1) {
            throw new IllegalArgumentException("Invalid property " + nameValue + " should be of form name:Value");
        }
        this.name = nameValue.substring(0, colonPos);
        String tempValue = nameValue.substring(colonPos + 1).trim();
        final char bChar = tempValue.charAt(0);
        final char eChar = tempValue.charAt(tempValue.length() - 1);
        if ((bChar == '\'' && eChar == '\'') || (bChar == '\"' && eChar == '\"')) {
            tempValue = tempValue.substring(1, tempValue.length() - 1);
        }
        this.value = tempValue;
    }
    
    public Property(final String name, final Object value) {
        this.name = name;
        this.value = value;
    }
    
    @Override
    public String toString() {
        return "Prop(" + this.name + ":" + this.value + ")";
    }
    
    @Override
    public boolean equals(final Object obj) {
        final Property prop = (Property)obj;
        return prop.name == this.name && prop.value == this.value;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.name).append(this.value).toHashCode();
    }
}
