package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "fetch-type")
@XmlEnum
public enum FetchType
{
    LAZY, 
    EAGER;
    
    public String value() {
        return this.name();
    }
    
    public static FetchType fromValue(final String v) {
        return valueOf(v);
    }
}
