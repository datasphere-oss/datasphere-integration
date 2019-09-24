package com.datasphere.wizard;
/*
 * CDC 向导创建接口
 */
public interface ICDCWizard
{
    public static final long TIME_TO_DAIT_FROM_AGENT = 10000L;
    public static final int DAIT_COUNTER = 15;
    
    String validateConnection(final String p0, final String p1, final String p2, final String p3);
    
    String checkPrivileges(final String p0, final String p1, final String p2, final String p3);
    
    String getTables(final String p0, final String p1, final String p2, final String p3, final String p4) throws Exception;
    
    String getTableColumns(final String p0, final String p1, final String p2, final String p3, final String p4) throws Exception;
    
    String checkCDCConfigurations(final String p0, final String p1, final String p2, final String p3);
    
    String checkVersion(final String p0, final String p1, final String p2, final String p3);
    
    public enum DBTYPE
    {
        LOGMINER, 
        XSTREAM, 
        MSSQL, 
        MYSQL, 
        ORACLE;
    }
}
