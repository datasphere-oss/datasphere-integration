package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "discriminator-type")
@XmlEnum
public enum DiscriminatorType
{
    STRING, 
    CHAR, 
    INTEGER;
    
    public String value() {
        return this.name();
    }
    
    public static DiscriminatorType fromValue(final String v) {
        return valueOf(v);
    }
}
