package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.fasterxml.jackson.core.type.*;
import org.eclipse.persistence.sessions.*;
import java.math.*;
import org.eclipse.persistence.core.sessions.*;

public class IntConverter extends PropertyDefConverter
{
    private static Logger logger;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Integer>() {};
    }
    
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return null;
        }
        if (dataValue != null && dataValue instanceof Integer) {
            return dataValue;
        }
        if (dataValue != null && dataValue instanceof BigDecimal) {
            final BigDecimal decimal = (BigDecimal)dataValue;
            return decimal.intValue();
        }
        return super.convertDataValueToObjectValue(dataValue, session);
    }
    
    static {
        IntConverter.logger = Logger.getLogger((Class)LongConverter.class);
    }
}
