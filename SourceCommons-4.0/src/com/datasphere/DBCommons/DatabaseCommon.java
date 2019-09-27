package com.datasphere.DBCommons;

import org.apache.log4j.*;
import java.util.*;
import org.json.*;

import com.datasphere.source.WizardCommons.*;

import java.sql.*;

public class DatabaseCommon
{
    Logger logger;
    Connection dbConnection;
    
    public DatabaseCommon(final Connection dbCon) {
        this.logger = Logger.getLogger((Class)DatabaseCommon.class);
        this.dbConnection = dbCon;
    }
    
    public List<String> getTables(final String fulltableName, final String dbType) throws Exception {
        final List<String> tables = new ArrayList<String>();
        String catalog = null;
        String schema = null;
        String table = null;
        try {
            final String[] split;
            final String[] tablesArray = split = fulltableName.split(";");
            for (final String tbl : split) {
                final String tableName = tbl.trim();
                if (tableName.contains(".")) {
                    final StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
                    if (tokenizer.countTokens() > 3) {
                        throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tableName + "'");
                    }
                    if (tokenizer.countTokens() == 3) {
                        catalog = tokenizer.nextToken();
                    }
                    schema = tokenizer.nextToken();
                    table = tokenizer.nextToken();
                }
                else {
                    table = tableName;
                }
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Trying to fetch table metadata for catalog '" + catalog + "' and schema '" + schema + "' and table pattern '" + table + "'"));
                }
                final DatabaseMetaData md = this.dbConnection.getMetaData();
                final ResultSet tableResultSet = md.getTables(catalog, schema, table, new String[] { "TABLE" });
                while (tableResultSet.next()) {
                    final String p1 = tableResultSet.getString(1);
                    final String p2 = tableResultSet.getString(2);
                    final String p3 = tableResultSet.getString(3);
                    final StringBuilder tableFQN = new StringBuilder();
                    if (p1 != null) {
                        tableFQN.append(p1 + ".");
                    }
                    if (p2 != null) {
                        tableFQN.append(p2 + ".");
                    }
                    tableFQN.append(p3);
                    tables.add(tableFQN.toString());
                    if (this.logger.isInfoEnabled()) {
                        this.logger.info((Object)("Adding table " + tableFQN.toString() + " to the list of tables to be queried"));
                    }
                }
            }
        }
        catch (Exception e) {
            final String errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause() + ";Message : " + e.getMessage();
            final Exception exception = new Exception(errorString);
            this.logger.error((Object)errorString);
            throw exception;
        }
        return tables;
    }
    
    public Map getTableColumns(final String TableNames) throws JSONException, SQLException {
        final JSONArray newJArray = new JSONArray(TableNames);
        final LinkedHashMap tablesAndCol = new LinkedHashMap();
        String catalog = null;
        String schema = null;
        String table = null;
        String columnName = "";
        if (newJArray != null) {
            for (int arrLen = newJArray.length(), i = 0; i < arrLen; ++i) {
                final LinkedHashMap colAndType = new LinkedHashMap();
                final ArrayList<String> allFields = new ArrayList<String>();
                final String tn = newJArray.getString(i);
                if (tn.contains(".")) {
                    final StringTokenizer tokenizer = new StringTokenizer(tn, ".");
                    if (tokenizer.countTokens() > 3) {
                        throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tn + "'");
                    }
                    if (tokenizer.countTokens() == 3) {
                        catalog = tokenizer.nextToken();
                    }
                    schema = tokenizer.nextToken();
                    table = tokenizer.nextToken();
                }
                else {
                    table = tn;
                }
                try {
                    final DatabaseMetaData md = this.dbConnection.getMetaData();
                    final ResultSet colResultSet = md.getColumns(catalog, schema, table, null);
                    while (colResultSet.next()) {
                        columnName = colResultSet.getString("COLUMN_NAME");
                        final String dataType = colResultSet.getString("TYPE_NAME");
                        colAndType.put(columnName, dataType);
                    }
                }
                catch (SQLException e) {
                    throw e;
                }
                tablesAndCol.put(tn, colAndType);
            }
        }
        return tablesAndCol;
    }
    
    public boolean checkVersion(final String versionToCheck, final String dbType) throws Exception {
        PreparedStatement ps = null;
        ResultSet OVersion = null;
        Boolean correctVersion = false;
        final String[] listOfVersions = versionToCheck.split(",");
        try {
            final String lowerCase = dbType.toLowerCase();
            switch (lowerCase) {
                case "oracle": {
                    ps = this.dbConnection.prepareStatement(wizardSQL.OracleVersionCheck);
                    break;
                }
                case "mssql": {
                    ps = this.dbConnection.prepareStatement(wizardSQL.MSVersionCheck);
                    break;
                }
            }
            OVersion = ps.executeQuery();
            if (OVersion.next()) {
                final String ver = OVersion.getString(1);
                for (int i = 0; i < listOfVersions.length; ++i) {
                    if (ver.toLowerCase().contains(listOfVersions[i].toLowerCase())) {
                        correctVersion = true;
                    }
                }
                if (!correctVersion) {
                    if (this.logger.isInfoEnabled()) {
                        this.logger.info((Object)("version mismatch. Version found: " + ver));
                    }
                    throw new Exception("Version Mismatch. Version supported: " + ver);
                }
            }
            if (OVersion != null) {
                OVersion.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
        catch (SQLException e) {
            throw e;
        }
        return correctVersion;
    }
}
