package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "enum-type")
@XmlEnum
public enum EnumType
{
    ORDINAL, 
    STRING;
    
    public String value() {
        return this.name();
    }
    
    public static EnumType fromValue(final String v) {
        return valueOf(v);
    }
}
