package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "parameter-mode")
@XmlEnum
public enum ParameterMode
{
    IN, 
    OUT, 
    INOUT, 
    REF_CURSOR;
    
    public String value() {
        return this.name();
    }
    
    public static ParameterMode fromValue(final String v) {
        return valueOf(v);
    }
}
