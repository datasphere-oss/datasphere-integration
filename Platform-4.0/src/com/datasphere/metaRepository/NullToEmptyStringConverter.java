package com.datasphere.metaRepository;

import org.eclipse.persistence.sessions.*;
import com.fasterxml.jackson.core.type.*;
import org.eclipse.persistence.core.sessions.*;

public class NullToEmptyStringConverter extends PropertyDefConverter
{
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return "";
        }
        return super.convertDataValueToObjectValue(dataValue, session);
    }
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<String>() {};
    }
}
