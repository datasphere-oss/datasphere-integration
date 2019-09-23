package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "tenant-table-discriminator-type")
@XmlEnum
public enum TenantTableDiscriminatorType
{
    SCHEMA, 
    SUFFIX, 
    PREFIX;
    
    public String value() {
        return this.name();
    }
    
    public static TenantTableDiscriminatorType fromValue(final String v) {
        return valueOf(v);
    }
}
