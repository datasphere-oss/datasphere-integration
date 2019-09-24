package com.datasphere.wizard;

import com.fasterxml.jackson.databind.*;
import org.apache.log4j.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.core.*;

public class ValidationResult
{
    public static final ObjectMapper mapper;
    private static Logger logger;
    public boolean result;
    public String message;
    
    public ValidationResult() {
        this.result = true;
        this.message = null;
    }
    
    public ValidationResult(final boolean result, final String message) {
        this.result = true;
        this.message = null;
        this.result = result;
        this.message = message;
    }
    
    public String toJson() {
        try {
            return ValidationResult.mapper.writeValueAsString((Object)this);
        }
        catch (JsonProcessingException jex) {
            ValidationResult.logger.warn((Object)("error converting json " + jex.getMessage()));
            return "<error converting json>";
        }
    }
    
    static {
        mapper = ObjectMapperFactory.newInstance();
        ValidationResult.logger = Logger.getLogger((Class)ValidationResult.class);
    }
}
