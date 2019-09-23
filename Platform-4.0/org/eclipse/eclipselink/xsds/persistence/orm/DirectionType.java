package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "direction-type")
@XmlEnum
public enum DirectionType
{
    IN, 
    OUT, 
    IN_OUT, 
    OUT_CURSOR;
    
    public String value() {
        return this.name();
    }
    
    public static DirectionType fromValue(final String v) {
        return valueOf(v);
    }
}
