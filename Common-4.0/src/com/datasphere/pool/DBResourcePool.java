package com.datasphere.pool;

public interface DBResourcePool<T> extends ResourcePool<T>
{
    void setURL(final String p0);
    
    void setUserName(final String p0);
    
    void setPassword(final String p0);
    
    void setDriverName(final String p0);
    
    void setDriverFor(final DB_TYPE p0);
    
    public enum DB_TYPE
    {
        MYSQL, 
        MSSQL, 
        ORACLE, 
        POSTGRESQL, 
        DERBY, 
        SYBASE, 
        OTHERS;
    }
}
