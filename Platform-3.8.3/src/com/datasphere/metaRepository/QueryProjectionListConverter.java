package com.datasphere.metaRepository;

import org.apache.log4j.*;

import com.datasphere.event.*;
import com.datasphere.intf.*;
import com.fasterxml.jackson.core.type.*;
import java.util.*;

import org.eclipse.persistence.sessions.*;

import java.io.*;
import com.fasterxml.jackson.databind.*;
import org.eclipse.persistence.core.sessions.*;

public class QueryProjectionListConverter extends PropertyDefConverter
{
    private static Logger logger;
    private static final long serialVersionUID = -8400939726819076422L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<QueryManager.QueryProjection>>() {};
    }
    
    @Override
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        final ObjectMapper mapper = ObjectMapperFactory.newInstance();
        try {
            final String json = mapper.writeValueAsString(objectValue);
            if (QueryProjectionListConverter.logger.isTraceEnabled()) {
                QueryProjectionListConverter.logger.trace((Object)("Converted rsfield " + objectValue + " to " + json));
            }
            return json;
        }
        catch (IOException e) {
            QueryProjectionListConverter.logger.error((Object)e);
            return null;
        }
    }
    
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            try {
                final ObjectMapper mapper = ObjectMapperFactory.newInstance();
                final Object values = mapper.readValue((String)dataValue, (TypeReference)this.getTypeReference());
                return values;
            }
            catch (IOException e) {
                QueryProjectionListConverter.logger.error((Object)e);
                return null;
            }
        }
        return null;
    }
    
    static {
        QueryProjectionListConverter.logger = Logger.getLogger((Class)QueryProjectionListConverter.class);
    }
}
