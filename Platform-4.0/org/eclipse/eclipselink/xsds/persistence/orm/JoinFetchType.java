package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "join-fetch-type")
@XmlEnum
public enum JoinFetchType
{
    INNER, 
    OUTER;
    
    public String value() {
        return this.name();
    }
    
    public static JoinFetchType fromValue(final String v) {
        return valueOf(v);
    }
}
