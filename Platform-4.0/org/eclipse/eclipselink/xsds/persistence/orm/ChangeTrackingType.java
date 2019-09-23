package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "change-tracking-type")
@XmlEnum
public enum ChangeTrackingType
{
    ATTRIBUTE, 
    OBJECT, 
    DEFERRED, 
    AUTO;
    
    public String value() {
        return this.name();
    }
    
    public static ChangeTrackingType fromValue(final String v) {
        return valueOf(v);
    }
}
