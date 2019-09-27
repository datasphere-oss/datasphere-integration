package com.datasphere.source.WizardCommons;

import java.util.*;
import org.json.*;

import com.datasphere.DBCommons.*;

import java.sql.*;

public class WizCommons
{
    static Connection dbConnection;
    DatabaseCommon newCom;
    
    public WizCommons() {
    }
    
    public WizCommons(final Connection conn) {
        WizCommons.dbConnection = conn;
        this.newCom = new DatabaseCommon(WizCommons.dbConnection);
    }
    
    public String getTables(final String fulltableName, final String dbType) throws Exception {
        final List<String> tables = this.newCom.getTables(fulltableName, dbType);
        final JSONArray jsonAray = new JSONArray((Collection)tables);
        final String JSS = jsonAray.toString();
        return JSS;
    }
    
    public String getTableColumns(final String TableNames) throws JSONException, SQLException {
        final Map tablesAndCol = this.newCom.getTableColumns(TableNames);
        final JSONObject tableMap = new JSONObject(tablesAndCol);
        final String Stringmap = tableMap.toString();
        return Stringmap;
    }
    
    public boolean checkVersion(final String versionToCheck, final String dbType) throws Exception {
        return this.newCom.checkVersion(versionToCheck, dbType);
    }
    
    static {
        WizCommons.dbConnection = null;
    }
}
