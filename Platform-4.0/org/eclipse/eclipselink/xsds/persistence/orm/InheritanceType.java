package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "inheritance-type")
@XmlEnum
public enum InheritanceType
{
    SINGLE_TABLE, 
    JOINED, 
    TABLE_PER_CLASS;
    
    public String value() {
        return this.name();
    }
    
    public static InheritanceType fromValue(final String v) {
        return valueOf(v);
    }
}
