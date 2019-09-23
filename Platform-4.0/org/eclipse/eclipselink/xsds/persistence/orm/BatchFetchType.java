package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "batch-fetch-type")
@XmlEnum
public enum BatchFetchType
{
    JOIN, 
    EXISTS, 
    IN;
    
    public String value() {
        return this.name();
    }
    
    public static BatchFetchType fromValue(final String v) {
        return valueOf(v);
    }
}
