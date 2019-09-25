package com.datasphere.Connection;

import java.sql.*;

import com.datasphere.exception.*;

public class PostgresConnection extends DatabaseWriterConnection
{
    protected PostgresConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        super(dbUrl, dbUser, dbPasswd);
    }
    
    @Override
    protected String getCatalog(final Connection conn) throws DatabaseWriterException {
        String currentCatalog = null;
        final String dbQuery = "SELECT current_database();";
        try {
            final PreparedStatement schemaStmt = conn.prepareStatement(dbQuery);
            final ResultSet schemaResultset = schemaStmt.executeQuery();
            while (schemaResultset.next()) {
                currentCatalog = schemaResultset.getString(1);
            }
        }
        catch (SQLException e) {
            throw new DatabaseWriterException("Got exception while fetching current schema {" + e.getMessage() + "}", e);
        }
        return currentCatalog;
    }
    
    @Override
    protected String getSchema(final Connection conn) throws DatabaseWriterException {
        String currentSchema = null;
        final String dbQuery = "SELECT current_schema();";
        try {
            final PreparedStatement schemaStmt = conn.prepareStatement(dbQuery);
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
    public TableNameParts getTableNameParts(final String tableName) {
        final TableNameParts tps = super.getTableNameParts(tableName);
        if (tps.catalog != null) {
            tps.catalog = tps.catalog.toLowerCase();
        }
        if (tps.schema != null) {
            tps.schema = tps.schema.toLowerCase();
        }
        if (tps.table != null) {
            tps.table = tps.table.toLowerCase();
        }
        return tps;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
