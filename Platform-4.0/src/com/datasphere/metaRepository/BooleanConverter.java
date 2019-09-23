package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.fasterxml.jackson.core.type.*;
import org.eclipse.persistence.sessions.*;
import org.eclipse.persistence.core.sessions.*;

public class BooleanConverter extends PropertyDefConverter
{
    private static Logger logger;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Boolean>() {};
    }
    
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return null;
        }
        if (dataValue != null && dataValue instanceof Boolean) {
            return dataValue;
        }
        return super.convertDataValueToObjectValue(dataValue, session);
    }
    
    static {
        BooleanConverter.logger = Logger.getLogger((Class)BooleanConverter.class);
    }
}
