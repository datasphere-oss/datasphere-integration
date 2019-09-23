package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "temporal-type")
@XmlEnum
public enum TemporalType
{
    DATE, 
    TIME, 
    TIMESTAMP;
    
    public String value() {
        return this.name();
    }
    
    public static TemporalType fromValue(final String v) {
        return valueOf(v);
    }
}
