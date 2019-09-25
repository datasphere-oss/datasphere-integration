package com.datasphere.Connection;

import java.sql.*;

import com.datasphere.exception.*;

public class OracleConnection extends DatabaseWriterConnection
{
    protected OracleConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        super(dbUrl, dbUser, dbPasswd);
    }
    
    public String getSchema(final Connection conn) throws DatabaseWriterException {
        String currentSchema = null;
        final String schemaSelectQuery = "select sys_context( 'userenv', 'current_schema' ) from dual";
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
        return (currentSchema != null) ? currentSchema.toUpperCase() : null;
    }
    
    @Override
    protected String getCatalog(final Connection conn) throws DatabaseWriterException {
        String currentCatalog = null;
        final String catalogQuery = "select name from v$pdbs";
        try {
            final PreparedStatement catalogStmt = conn.prepareStatement(catalogQuery);
            final ResultSet catalogResultset = catalogStmt.executeQuery();
            while (catalogResultset.next()) {
                currentCatalog = catalogResultset.getString(1);
            }
        }
        catch (SQLException exp) {
            if (exp.getErrorCode() != 942) {
                throw new DatabaseWriterException("Got exception while fetching pdb name {" + exp.getMessage() + "}", exp);
            }
        }
        return currentCatalog;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
