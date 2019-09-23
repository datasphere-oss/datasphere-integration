package com.datasphere.recovery;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class PathItemListConverter implements Converter
{
    public static final long serialVersionUID = 2749988897547956681L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue instanceof Path.ItemList) {
            return ((Path.ItemList)objectValue).toString();
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue instanceof String) {
            return Path.ItemList.fromString((String)dataValue);
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
}
