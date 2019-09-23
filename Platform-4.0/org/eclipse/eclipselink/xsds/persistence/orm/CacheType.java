package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "cache-type")
@XmlEnum
public enum CacheType
{
    FULL, 
    WEAK, 
    SOFT, 
    SOFT_WEAK, 
    HARD_WEAK, 
    CACHE, 
    NONE;
    
    public String value() {
        return this.name();
    }
    
    public static CacheType fromValue(final String v) {
        return valueOf(v);
    }
}
