package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "cache-coordination-type")
@XmlEnum
public enum CacheCoordinationType
{
    SEND_OBJECT_CHANGES, 
    INVALIDATE_CHANGED_OBJECTS, 
    SEND_NEW_OBJECTS_WITH_CHANGES, 
    NONE;
    
    public String value() {
        return this.name();
    }
    
    public static CacheCoordinationType fromValue(final String v) {
        return valueOf(v);
    }
}
