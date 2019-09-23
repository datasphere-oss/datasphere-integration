package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "generation-type")
@XmlEnum
public enum GenerationType
{
    TABLE, 
    SEQUENCE, 
    IDENTITY, 
    AUTO;
    
    public String value() {
        return this.name();
    }
    
    public static GenerationType fromValue(final String v) {
        return valueOf(v);
    }
}
