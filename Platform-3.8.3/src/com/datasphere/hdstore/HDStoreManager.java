package com.datasphere.hdstore;

import com.datasphere.hdstore.constants.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.exceptions.*;

public interface HDStoreManager
{
    String getInstanceName();
    
    String getProviderName();
    
    Set<Capability> getCapabilities();
    
    CheckpointManager getCheckpointManager();
    
    String translateName(final NameType p0, final String p1);
    
    String[] getNames();
    
    HDStore get(final String p0, final Map<String, Object> p1);
    
    HDStore getUsingAlias(final String p0, final Map<String, Object> p1);
    
    boolean remove(final String p0);
    
    boolean removeUsingAlias(final String p0);
    
    HDStore create(final String p0, final Map<String, Object> p1);
    
    HDStore getOrCreate(final String p0, final Map<String, Object> p1);
    
    HDQuery prepareQuery(final JsonNode p0) throws HDStoreException;
    
    long delete(final JsonNode p0) throws HDStoreException;
    
    void flush();
    
    void fsync();
    
    void close();
    
    void shutdown();
}
