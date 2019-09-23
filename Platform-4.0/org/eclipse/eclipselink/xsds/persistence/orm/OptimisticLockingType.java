package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "optimistic-locking-type")
@XmlEnum
public enum OptimisticLockingType
{
    ALL_COLUMNS, 
    CHANGED_COLUMNS, 
    SELECTED_COLUMNS, 
    VERSION_COLUMN;
    
    public String value() {
        return this.name();
    }
    
    public static OptimisticLockingType fromValue(final String v) {
        return valueOf(v);
    }
}
