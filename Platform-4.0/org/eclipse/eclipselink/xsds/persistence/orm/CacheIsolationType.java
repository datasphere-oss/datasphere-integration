package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "cache-isolation-type")
@XmlEnum
public enum CacheIsolationType
{
    SHARED, 
    PROTECTED, 
    ISOLATED;
    
    public String value() {
        return this.name();
    }
    
    public static CacheIsolationType fromValue(final String v) {
        return valueOf(v);
    }
}
