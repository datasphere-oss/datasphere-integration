package com.datasphere.Connection;

import java.sql.*;

import com.datasphere.exception.*;

public class MSSQLConnection extends DatabaseWriterConnection
{
    protected MSSQLConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        super(dbUrl, dbUser, dbPasswd);
    }
    
    public String getSchema(final Connection conn) throws DatabaseWriterException {
        String currentSchema = null;
        final String schemaSelectQuery = "SELECT SCHEMA_NAME()";
        try {
            final PreparedStatement schemaStmt = conn.prepareStatement(schemaSelectQuery);
            final ResultSet schemaResultset = schemaStmt.executeQuery();
            while (schemaResultset.next()) {
                currentSchema = schemaResultset.getString(1);
            }
        }
        catch (SQLException e) {
            throw new DatabaseWriterException("Got exception while fetching current schema {" + e.getMessage() + "}", e);
        }
        return currentSchema;
    }
    
    @Override
    protected String getCatalog(final Connection conn) throws DatabaseWriterException {
        String currentCatalog = null;
        final String dbQuery = "SELECT DB_NAME()";
        try {
            final PreparedStatement schemaStmt = conn.prepareStatement(dbQuery);
            final ResultSet schemaResultset = schemaStmt.executeQuery();
            while (schemaResultset.next()) {
                currentCatalog = schemaResultset.getString(1);
            }
        }
        catch (SQLException e) {
            throw new DatabaseWriterException("Got exception while fetching current catalog {" + e.getMessage() + "}", e);
        }
        return currentCatalog;
    }
    
    @Override
    public TableNameParts getTableNameParts(final String tableName) {
        String schema = null;
        String catalog = null;
        String table = null;
        final String[] part = tableName.trim().split("\\.");
        if (part.length >= 1) {
            if (part.length >= 2) {
                if (part.length >= 3) {
                    if (part.length == 3) {
                        catalog = part[0];
                        schema = part[1];
                        table = part[2];
                    }
                }
                else {
                    schema = part[0];
                    table = part[1];
                }
            }
            else {
                table = part[0];
            }
        }
        if (catalog != null && catalog.startsWith("\"") && catalog.endsWith("\"")) {
            catalog = catalog.substring(1, catalog.length() - 1);
        }
        if (schema != null && schema.startsWith("\"") && schema.endsWith("\"")) {
            schema = schema.substring(1, schema.length() - 1);
        }
        if (table != null && table.startsWith("\"") && table.endsWith("\"")) {
            table = table.substring(1, table.length() - 1);
        }
        return new TableNameParts(catalog, schema, table);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
