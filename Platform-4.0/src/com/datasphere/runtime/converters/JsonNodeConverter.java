package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import org.eclipse.persistence.sessions.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class JsonNodeConverter implements Converter
{
    private static Logger logger;
    private static final long serialVersionUID = -1814149577596807517L;
    private final ObjectMapper jsonMapper;
    
    public JsonNodeConverter() {
        this.jsonMapper = ObjectMapperFactory.newInstance();
    }
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        if (objectValue instanceof JsonNode) {
            return ((JsonNode)objectValue).toString();
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String && !((String)dataValue).isEmpty()) {
            try {
                return this.jsonMapper.readTree((String)dataValue);
            }
            catch (Exception ex) {
                JsonNodeConverter.logger.error((Object)("error building jsonnode from string : " + (String)dataValue));
            }
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping mapping, final Session session) {
        if (mapping != null) {
            mapping.getField().setType((Class)String.class);
            mapping.getField().setSqlType(12);
            mapping.getField().setLength(6500);
        }
    }
    
    static {
        JsonNodeConverter.logger = Logger.getLogger((Class)JsonNodeConverter.class);
    }
}
