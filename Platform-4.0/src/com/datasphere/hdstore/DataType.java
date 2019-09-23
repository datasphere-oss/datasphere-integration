package com.datasphere.hdstore;

import com.fasterxml.jackson.databind.*;
import com.github.fge.jsonschema.main.*;
import java.util.*;
import com.datasphere.persistence.*;

public interface DataType
{
    HDStore getHDStore();
    
    String getName();
    
    JsonNode getSchemaJson();
    
    JsonSchema getDataSchema();
    
    boolean isValid();
    
    boolean isValid(final HD p0);
    
    void queue(final HD p0);
    
    void queue(final Iterable<HD> p0);
    
    boolean insert(final HD p0);
    
    boolean insert(final List<HD> p0, final HStore p1);
    
    void flush();
    
    void fsync();
}
