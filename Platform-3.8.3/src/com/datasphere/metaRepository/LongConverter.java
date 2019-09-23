package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.fasterxml.jackson.core.type.*;
import org.eclipse.persistence.sessions.*;
import org.eclipse.persistence.core.sessions.*;

public class LongConverter extends PropertyDefConverter
{
    private static Logger logger;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Long>() {};
    }
    
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return null;
        }
        if (dataValue != null && dataValue instanceof Long) {
            return dataValue;
        }
        return super.convertDataValueToObjectValue(dataValue, session);
    }
    
    static {
        LongConverter.logger = Logger.getLogger((Class)LongConverter.class);
    }
}
