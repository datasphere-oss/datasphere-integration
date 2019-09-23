package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "access-type")
@XmlEnum
public enum AccessType
{
    PROPERTY, 
    FIELD, 
    VIRTUAL;
    
    public String value() {
        return this.name();
    }
    
    public static AccessType fromValue(final String v) {
        return valueOf(v);
    }
}
