package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.runtime.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class IntervalConverter implements Converter
{
    private static final long serialVersionUID = -5587994974810478971L;
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session arg1) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            final Long lval = new Long((String)dataValue);
            return new Interval(lval);
        }
        return null;
    }
    
    public Object convertObjectValueToDataValue(final Object iPolicy, final Session arg1) {
        if (iPolicy == null) {
            return null;
        }
        if (iPolicy instanceof Interval) {
            return Long.toString(((Interval)iPolicy).value);
        }
        return null;
    }
    
    public void initialize(final DatabaseMapping arg0, final Session arg1) {
    }
    
    public boolean isMutable() {
        return false;
    }
}
