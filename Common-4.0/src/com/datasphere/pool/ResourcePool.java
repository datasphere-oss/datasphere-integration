package com.datasphere.pool;

import java.util.*;

public interface ResourcePool<T>
{
    public static final long ONE_SEC_IN_NANOS = 1000000000L;
    
    T createResource() throws ResourceException;
    
    boolean closeResource(final T p0) throws ResourceException;
    
    T getResource() throws ResourceException;
    
    boolean returnResource(final T p0, final boolean p1) throws ResourceException;
    
    boolean closeAllResource(final boolean p0) throws ResourceException;
    
    void setProperties(final Properties p0);
    
    Properties getProperties();
    
    void setMaxConnections(final int p0);
    
    void setMinConnections(final int p0);
    
    boolean validateProp();
    
    void setRetryPolicy(final RetryPolicy p0);
    
    int size();
    
    void setClassLoader(final ClassLoader p0);
}
