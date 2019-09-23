package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "cache-key-type")
@XmlEnum
public enum CacheKeyType
{
    ID_VALUE, 
    CACHE_KEY, 
    AUTO;
    
    public String value() {
        return this.name();
    }
    
    public static CacheKeyType fromValue(final String v) {
        return valueOf(v);
    }
}
