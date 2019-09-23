package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "order-column-correction-type")
@XmlEnum
public enum OrderColumnCorrectionType
{
    READ, 
    READ_WRITE, 
    EXCEPTION;
    
    public String value() {
        return this.name();
    }
    
    public static OrderColumnCorrectionType fromValue(final String v) {
        return valueOf(v);
    }
}
