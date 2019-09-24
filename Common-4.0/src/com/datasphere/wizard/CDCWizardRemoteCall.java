package com.datasphere.wizard;

import java.io.*;
import java.lang.reflect.*;

import com.datasphere.uuid.*;

public class CDCWizardRemoteCall implements Serializable
{
    public DatabaseType databaseType;
    private UUID sequenceID;
    public String agentName;
    public String functionName;
    public Object[] args;
    public Object member;
    
    public CDCWizardRemoteCall(final Object member, final DatabaseType databaseType, final String agentName, final String functionName, final Object[] args) {
        this.databaseType = DatabaseType.UNKNOWN;
        this.sequenceID = UUID.genCurTimeUUID();
        this.agentName = null;
        this.functionName = null;
        this.args = null;
        this.member = null;
        this.databaseType = databaseType;
        this.agentName = agentName;
        this.functionName = functionName;
        this.args = args;
        this.member = member;
    }
    
    public String call(final ClassLoader classLoader) throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException {
        if (this.databaseType == DatabaseType.UNKNOWN) {
            return null;
        }
        final Class cdcWizardClass = classLoader.loadClass(this.databaseType.getClassName());
        final ICDCWizard cdcWizard = (ICDCWizard)cdcWizardClass.newInstance();
        final Method[] allMethods = cdcWizardClass.getDeclaredMethods();
        String validationResult = null;
        for (final Method method : allMethods) {
            if (method.getName().equalsIgnoreCase(this.functionName)) {
                validationResult = (String)method.invoke(cdcWizard, this.args);
            }
        }
        return validationResult;
    }
    
    public UUID getSequenceID() {
        return this.sequenceID;
    }
    
    public enum DatabaseType
    {
        UNKNOWN(""), 
        ORACLE("com.datasphere.source.oraclecommon.OracleCDCWizard"), 
        MSSQL("com.datasphere.source.mssql.MSSqlWizard"), 
        MYSQL("com.datasphere.source.mysql.MySQLWizard");
        
        private String className;
        
        private DatabaseType(final String className) {
            this.className = className;
        }
        
        public String getClassName() {
            return this.className;
        }
    }
}
