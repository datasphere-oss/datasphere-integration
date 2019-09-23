package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import org.eclipse.persistence.sessions.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class ClassConverter implements Converter
{
    private static final long serialVersionUID = 6948468434440252794L;
    private static Logger logger;
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session arg1) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            try {
                return Class.forName((String)dataValue, false, ClassLoader.getSystemClassLoader());
            }
            catch (ClassNotFoundException e) {
                ClassConverter.logger.error((Object)e);
                return null;
            }
        }
        return null;
    }
    
    public Object convertObjectValueToDataValue(final Object classname, final Session arg1) {
        if (classname == null) {
            return null;
        }
        if (classname instanceof Class) {
            return ((Class)classname).getName();
        }
        return null;
    }
    
    public void initialize(final DatabaseMapping arg0, final Session arg1) {
    }
    
    public boolean isMutable() {
        return false;
    }
    
    static {
        ClassConverter.logger = Logger.getLogger((Class)ClassConverter.class);
    }
}
