package com.datasphere.Checkpoint;

import java.util.*;

import com.datasphere.Connection.*;

import java.sql.*;

public class PostgreSQLCheckpointTableImpl extends BaseCheckpointTableImpl
{
    Map<String, ColumnDetails> colDetails;
    
    protected PostgreSQLCheckpointTableImpl(final String table, final String target, final DatabaseWriterConnection dbConnection) throws SQLException {
        super(table, target, dbConnection);
        this.colDetails = new TreeMap<String, ColumnDetails>((Comparator)String.CASE_INSENSITIVE_ORDER) {
            {
                this.put("id", new ColumnDetails("id", 12, true));
                this.put("sourceposition", new ColumnDetails("sourceposition", -2, false));
                this.put("pendingddl", new ColumnDetails("pendingddl", 2, false));
                this.put("ddl", new ColumnDetails("ddl", 12, false));
            }
        };
    }
    
    @Override
    public Map<String, ColumnDetails> getColumnDetails() {
        return this.colDetails;
    }
    
    @Override
    public String createSQL() {
        return "create table chkpoint (id character varying(100) primary key, sourceposition bytea, pendingddl numeric(1), ddl text);";
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
