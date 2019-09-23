package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "data-format-type")
@XmlEnum
public enum DataFormatType
{
    XML, 
    INDEXED, 
    MAPPED;
    
    public String value() {
        return this.name();
    }
    
    public static DataFormatType fromValue(final String v) {
        return valueOf(v);
    }
}
