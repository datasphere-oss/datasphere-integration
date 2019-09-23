package com.datasphere.metaRepository;

public interface MetaDataDbProvider
{
    void setJDBCDriver();
    
    String getJDBCURL(final String p0, final String p1, final String p2, final String p3);
    
    String getJDBCDriver();
    
    Object getIntegerValueFromData(final Object p0);
    
    String askMetaDataRepositoryLocation(final String p0);
    
    String askDefaultDBLocationAppendedWithPort(final String p0);
    
    String askDBUserName(final String p0);
    
    String askDBPassword(final String p0);
    
    String askDBName(final String p0);
}
