package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "multitenant-type")
@XmlEnum
public enum MultitenantType
{
    SINGLE_TABLE, 
    VPD, 
    TABLE_PER_TENANT;
    
    public String value() {
        return this.name();
    }
    
    public static MultitenantType fromValue(final String v) {
        return valueOf(v);
    }
}
