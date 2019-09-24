package com.datasphere.event;

import com.fasterxml.jackson.datatype.joda.*;
import com.fasterxml.jackson.databind.*;

public class ObjectMapperFactory
{
    private static ObjectMapper mapper;
    
    private ObjectMapperFactory() {
        ObjectMapperFactory.mapper.registerModule((Module)new JodaModule());
        ObjectMapperFactory.mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
    }
    
    public static ObjectMapper getInstance() {
        return ObjectMapperFactory.mapper;
    }
    
    public static ObjectMapper newInstance() {
        final ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.registerModule((Module)new JodaModule());
        jsonMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        return jsonMapper;
    }
    
    static {
        ObjectMapperFactory.mapper = new ObjectMapper();
    }
}
