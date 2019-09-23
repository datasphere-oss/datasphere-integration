package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.uuid.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class UUIDConverter implements Converter
{
    public static final long serialVersionUID = 2749988897547956681L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        if (objectValue instanceof UUID) {
            return ((UUID)objectValue).toString();
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String && !((String)dataValue).isEmpty()) {
            final UUID u = new UUID();
            u.setUUIDString((String)dataValue);
            return u;
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
}
