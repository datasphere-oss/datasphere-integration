package com.datasphere.runtime.meta;

import java.util.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import java.io.*;

public class MetaInfoStatus implements Serializable
{
    private static final long serialVersionUID = -9174291694730145114L;
    private boolean isValid;
    private boolean isAnonymous;
    private boolean isDropped;
    private boolean isAdhoc;
    private boolean isGenerated;
    
    public MetaInfoStatus() {
        this.isValid = true;
        this.isAnonymous = false;
        this.isDropped = false;
        this.isAdhoc = false;
        this.isGenerated = false;
    }
    
    public boolean isValid() {
        return this.isValid;
    }
    
    public MetaInfoStatus setValid(final boolean isValid) {
        this.isValid = isValid;
        return this;
    }
    
    public boolean isAnonymous() {
        return this.isAnonymous;
    }
    
    public MetaInfoStatus setAnonymous(final boolean isAnonymous) {
        this.isAnonymous = isAnonymous;
        return this;
    }
    
    public boolean isDropped() {
        return this.isDropped;
    }
    
    public MetaInfoStatus setDropped(final boolean isDropped) {
        this.isDropped = isDropped;
        return this;
    }
    
    public boolean isAdhoc() {
        return this.isAdhoc;
    }
    
    public MetaInfoStatus setAdhoc(final boolean isAdhoc) {
        this.isAdhoc = isAdhoc;
        return this;
    }
    
    public MetaInfoStatus setGenerated(final boolean isGenerated) {
        this.isGenerated = isGenerated;
        return this;
    }
    
    public boolean isGenerated() {
        return this.isGenerated;
    }
    
    @Override
    public String toString() {
        return "{ \nisValid => " + this.isValid + " \nisAnonymous => " + this.isAnonymous + " \nisDropped => " + this.isDropped + " \nisAdhoc => " + this.isAdhoc() + " \nisGenerated => " + this.isGenerated() + " \n} ";
    }
    
    public static MetaInfoStatus jsonDeserializer(final Map results) throws JsonParseException, JsonMappingException, IOException {
        if (results == null || results.isEmpty()) {
            return null;
        }
        final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        jsonMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        final MetaInfoStatus mis = new MetaInfoStatus();
        if (results.get("isValid") != null) {
            mis.setValid((Boolean)results.get("isValid"));
        }
        if (results.get("isAnonymous") != null) {
            mis.setAnonymous((Boolean)results.get("isAnonymous"));
        }
        if (results.get("isDropped") != null) {
            mis.setDropped((Boolean)results.get("isDropped"));
        }
        if (results.get("isAdhoc") != null) {
            mis.setAdhoc((Boolean)results.get("isAdhoc"));
        }
        if (results.get("isGenerated") != null) {
            mis.setGenerated((Boolean)results.get("isGenerated"));
        }
        return mis;
    }
}
