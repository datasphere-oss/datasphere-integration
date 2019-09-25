package com.datasphere.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.Fetchers.Fetcher;
import com.datasphere.source.cdc.common.DatabaseTable;
import com.datasphere.source.lib.meta.DatabaseColumn;

public class Table extends DatabaseTable
{
    private Logger logger;
    protected DatabaseColumn[] columns;
    private String databaseName;
    protected String fullyQualifiedName;
    private boolean isValid;
    protected Fetcher fetcher;
    protected String insertSQLPart;
    protected String updateSQLPart;
    protected String deleteSQLPart;
    
    public Table() {
        super((String)null, (String)null);
        this.logger = LoggerFactory.getLogger((Class)Table.class);
    }
    
    public Table(final String tableName, final String fQualifiedName, final List<DBWriterColumn> cols) {
        super(tableName, (String)null);
        this.logger = LoggerFactory.getLogger((Class)Table.class);
        this.fullyQualifiedName = fQualifiedName;
        this.columns = new DatabaseColumn[cols.size()];
        for (int itr = 0; itr < cols.size(); ++itr) {
            this.columns[itr] = cols.get(itr);
        }
        this.setSQLPart();
    }
    
    public Table(final String tableName, final JSONObject tableDetails) throws JSONException {
        super((String)null, (String)null);
        this.logger = LoggerFactory.getLogger((Class)Table.class);
        this.fullyQualifiedName = tableName;
        final JSONArray columnDetails = tableDetails.getJSONArray("ColumnMetadata");
        final int noOfColumns = columnDetails.length();
        this.columns = new DatabaseColumn[noOfColumns];
        for (int itr = 0; itr < noOfColumns; ++itr) {
            final JSONObject colDetails = columnDetails.getJSONObject(itr);
            this.columns[itr] = new DBWriterColumn(colDetails);
        }
        this.setSQLPart();
    }
    
    public Table(final ResultSet tableInfo, final ResultSet metaResultSet, final ResultSet keyResultSet) throws SQLException {
        super((String)null, (String)null);
        this.logger = LoggerFactory.getLogger((Class)Table.class);
        this.fetchTableInfo(tableInfo);
        this.fetchColumnInformation(metaResultSet);
        this.fetchKeyInfo(keyResultSet);
        if (this.logger.isDebugEnabled()) {
            final StringBuilder tableDetails = new StringBuilder();
            tableDetails.append("Table Name {" + this.fullyQualifiedName + "}");
            for (int itr = 0; itr < this.columns.length; ++itr) {
                tableDetails.append("\t" + this.columns[itr].toString() + "\n");
            }
            this.logger.debug(tableDetails.toString());
        }
        this.setSQLPart();
    }
    
    private void fetchColumnInformation(final ResultSet columnInfo) throws SQLException {
        final ArrayList<DatabaseColumn> listOfColumns = new ArrayList<DatabaseColumn>();
        while (columnInfo.next()) {
            final DBWriterColumn column = new DBWriterColumn(columnInfo);
            listOfColumns.add(column);
        }
        this.columns = new DatabaseColumn[listOfColumns.size()];
        for (int itr = 0; itr < listOfColumns.size(); ++itr) {
            this.columns[itr] = listOfColumns.get(itr);
        }
    }
    
    private void fetchKeyInfo(final ResultSet keyResultSet) throws SQLException {
        while (keyResultSet.next()) {
            final String nameOfTheKeyColumn = keyResultSet.getString("COLUMN_NAME");
            for (int itr = 0; itr < this.columns.length; ++itr) {
                if (this.columns[itr].getName().equals(nameOfTheKeyColumn)) {
                    this.columns[itr].setKey(true);
                }
            }
        }
    }
    
    private void fetchTableInfo(final ResultSet tableInfo) throws SQLException {
        while (tableInfo.next()) {
            this.isValid = true;
            final String catalog = tableInfo.getString(1);
            final String schema = tableInfo.getString(2);
            final String table = tableInfo.getString(3);
            this.setFullyQualifiedName(catalog, schema, table);
            this.setOwner(schema);
            this.setName(table);
        }
    }
    
    protected void setFullyQualifiedName(final String catalog, final String schema, final String table) {
        this.fullyQualifiedName = "";
        this.fullyQualifiedName = ((catalog == null || catalog.isEmpty()) ? "" : catalog);
        this.fullyQualifiedName += ((schema == null || schema.isEmpty()) ? "" : (this.fullyQualifiedName.isEmpty() ? schema : ("." + schema)));
        this.fullyQualifiedName += ((table == null || table.isEmpty()) ? "" : (this.fullyQualifiedName.isEmpty() ? table : ("." + table)));
    }
    
    protected void setSQLPart() {
        this.insertSQLPart = "INSERT INTO " + this.fullyQualifiedName + " ";
        this.updateSQLPart = "UPDATE " + this.fullyQualifiedName + " SET ";
        this.deleteSQLPart = "DELETE FROM " + this.fullyQualifiedName + " WHERE ";
    }
    
    public boolean isValid() {
        return this.isValid;
    }
    
    public DatabaseColumn[] getColumns() {
        return this.columns;
    }
    
    public void setColumns(final DatabaseColumn[] columns) {
        this.columns = columns;
    }
    
    public String getDatabaseName() {
        return this.databaseName;
    }
    
    public String getFullyQualifiedName() {
        return this.fullyQualifiedName;
    }
    
    public int getColumnCount() {
        return this.columns.length;
    }
    
    public String insertSQLPart() {
        return this.insertSQLPart;
    }
    
    public String updateSQLPart() {
        return this.updateSQLPart;
    }
    
    public String deleteSQLPart() {
        return this.deleteSQLPart;
    }
    
    public Map<String, DatabaseColumn> columnsAsMap() {
        final Map<String, DatabaseColumn> colMap = new TreeMap<String, DatabaseColumn>(String.CASE_INSENSITIVE_ORDER);
        for (int itr = 0; itr < this.columns.length; ++itr) {
            colMap.put(this.columns[itr].getName(), this.columns[itr]);
        }
        return colMap;
    }
    
    public void dataFetcher(final Fetcher fetcher) {
        this.fetcher = fetcher;
    }
    
    public Fetcher dataFetcher() {
        return this.fetcher;
    }
}
