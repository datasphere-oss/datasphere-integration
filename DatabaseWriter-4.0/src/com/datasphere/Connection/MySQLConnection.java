package com.datasphere.Connection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.exception.DatabaseWriterException;

public class MySQLConnection extends DatabaseWriterConnection
{
    private Logger logger;
    
    protected MySQLConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        super(dbUrl, dbUser, dbPasswd);
        this.logger = LoggerFactory.getLogger((Class)MySQLConnection.class);
    }
    
    @Override
    protected String getCatalog(final Connection conn) throws DatabaseWriterException {
        String currentCatalog = null;
        final String dbQuery = "SELECT DATABASE() FROM DUAL";
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
    public String fullyQualifiedName(final String table) {
        String fqn = "";
        final String[] parts = table.split("\\.");
        if (parts.length < 2 && this.catalog != null && !this.catalog.isEmpty()) {
            fqn = this.catalog;
        }
        if (fqn.isEmpty()) {
            fqn = table;
        }
        else {
            fqn = fqn + "." + table;
        }
        return fqn;
    }
    
    @Override
    public TableNameParts getTableNameParts(final String tableName) {
        final String schema = null;
        String catalog = null;
        String table = null;
        final String[] part = tableName.trim().split("\\.");
        if (part.length >= 1) {
            if (part.length >= 2) {
                if (part.length >= 3) {
                    this.logger.error("Invalid table name format {" + tableName + "} is specified. Three part name is not supported for this target database.");
                    catalog = part[part.length - 2];
                    table = part[part.length - 1];
                }
                else {
                    catalog = part[0];
                    table = part[1];
                }
            }
            else {
                table = part[0];
            }
        }
        return new TableNameParts(catalog, schema, table);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
