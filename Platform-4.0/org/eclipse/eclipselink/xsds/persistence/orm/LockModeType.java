package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "lock-mode-type")
@XmlEnum
public enum LockModeType
{
    READ, 
    WRITE, 
    OPTIMISTIC, 
    OPTIMISTIC_FORCE_INCREMENT, 
    PESSIMISTIC_READ, 
    PESSIMISTIC_WRITE, 
    PESSIMISTIC_FORCE_INCREMENT, 
    NONE;
    
    public String value() {
        return this.name();
    }
    
    public static LockModeType fromValue(final String v) {
        return valueOf(v);
    }
}
