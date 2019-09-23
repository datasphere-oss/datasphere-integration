package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "existence-type")
@XmlEnum
public enum ExistenceType
{
    CHECK_CACHE, 
    CHECK_DATABASE, 
    ASSUME_EXISTENCE, 
    ASSUME_NON_EXISTENCE;
    
    public String value() {
        return this.name();
    }
    
    public static ExistenceType fromValue(final String v) {
        return valueOf(v);
    }
}
