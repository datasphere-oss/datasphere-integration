package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import org.joda.time.*;
import java.util.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class JodaDateConverter2 implements Converter
{
    private static final long serialVersionUID = -4622230539751930045L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        if (objectValue instanceof DateTime) {
            return ((DateTime)objectValue).toDate();
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof Date) {
            return new DateTime((Object)dataValue);
        }
        if (dataValue instanceof String) {
            if (!((String)dataValue).isEmpty()) {
                final long l = Long.parseLong((String)dataValue);
                return new DateTime(l);
            }
        }
        else if (dataValue instanceof Long) {
            return new DateTime((long)dataValue);
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping mapping, final Session session) {
        if (mapping != null) {
            mapping.getField().setType((Class)Date.class);
            mapping.getField().setSqlType(93);
        }
    }
}
