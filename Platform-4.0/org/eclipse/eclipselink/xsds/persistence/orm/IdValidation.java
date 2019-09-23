package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "id-validation")
@XmlEnum
public enum IdValidation
{
    NULL, 
    ZERO, 
    NONE;
    
    public String value() {
        return this.name();
    }
    
    public static IdValidation fromValue(final String v) {
        return valueOf(v);
    }
}
