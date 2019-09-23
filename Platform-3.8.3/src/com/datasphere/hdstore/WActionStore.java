package com.datasphere.hdstore;

import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.persistence.*;

public interface HDStore
{
    HDStoreManager getManager();
    
    String getName();
    
    Map<String, Object> getProperties();
    
    Iterator<String> getNames();
    
    DataType get(final String p0, final Map<String, Object> p1);
    
    boolean remove(final String p0);
    
    DataType createDataType(final String p0, final JsonNode p1);
    
    DataType setDataType(final String p0, final JsonNode p1, final HStore p2);
    
    void flush();
    
    void fsync();
    
    long getHDCount();
}
