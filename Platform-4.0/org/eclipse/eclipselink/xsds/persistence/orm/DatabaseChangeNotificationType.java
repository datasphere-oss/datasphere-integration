package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlType(name = "database-change-notification-type")
@XmlEnum
public enum DatabaseChangeNotificationType
{
    NONE, 
    INVALIDATE;
    
    public String value() {
        return this.name();
    }
    
    public static DatabaseChangeNotificationType fromValue(final String v) {
        return valueOf(v);
    }
}
