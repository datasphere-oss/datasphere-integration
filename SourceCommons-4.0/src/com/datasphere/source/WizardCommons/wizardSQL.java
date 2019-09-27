package com.datasphere.source.WizardCommons;

public class wizardSQL
{
    public static String OracleVersionCheck;
    public static String MSVersionCheck;
    public static String OraclelogMode;
    
    static {
        wizardSQL.OracleVersionCheck = "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'";
        wizardSQL.MSVersionCheck = "SELECT @@version";
        wizardSQL.OraclelogMode = "select log_mode from v$database";
    }
}
