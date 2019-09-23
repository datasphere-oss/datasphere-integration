package com.datasphere.metaRepository;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import com.fasterxml.jackson.databind.*;
import org.eclipse.persistence.sessions.*;
import java.io.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.runtime.meta.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;
import com.datasphere.runtime.converters.*;

public class PropertyDefConverter implements Converter
{
    private static Logger logger;
    public static final long serialVersionUID = 2740988897547957681L;
    private static ObjectMapper mapper;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        try {
            final String json = PropertyDefConverter.mapper.writeValueAsString(objectValue);
            if (PropertyDefConverter.logger.isTraceEnabled()) {
                PropertyDefConverter.logger.trace((Object)("Converted property value " + objectValue + " to " + json));
            }
            return json;
        }
        catch (IOException e) {
            PropertyDefConverter.logger.error((Object)e);
            return null;
        }
    }
    
    TypeReference<?> getTypeReference() {
        return new TypeReference<Map<String, MetaInfo.PropertyDef>>() {};
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String && ((String)dataValue).trim().length() != 0) {
            try {
                final Object values = PropertyDefConverter.mapper.readValue((String)dataValue, (TypeReference)this.getTypeReference());
                return values;
            }
            catch (IOException e) {
                PropertyDefConverter.logger.error((Object)("Problem converting object: " + dataValue), (Throwable)e);
                return null;
            }
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
    
    static {
        PropertyDefConverter.logger = Logger.getLogger((Class)MapConverter.class);
        PropertyDefConverter.mapper = ObjectMapperFactory.newInstance();
    }
}
