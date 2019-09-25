package com.datasphere.Checkpoint;

import java.util.*;

import com.datasphere.Connection.*;

import java.sql.*;

public class MySQLCheckpointTableImpl extends BaseCheckpointTableImpl
{
    Map<String, ColumnDetails> colDetails;
    
    protected MySQLCheckpointTableImpl(final String table, final String target, final DatabaseWriterConnection dbConnection) throws SQLException {
        super(table, target, dbConnection);
        this.colDetails = new HashMap<String, ColumnDetails>() {
            {
                this.put("id", new ColumnDetails("id", 12, true));
                this.put("sourceposition", new ColumnDetails("sourceposition", -4, false));
                this.put("pendingddl", new ColumnDetails("pendingddl", -7, false));
                this.put("ddl", new ColumnDetails("ddl", -1, false));
            }
        };
    }
    
    @Override
    public Map<String, ColumnDetails> getColumnDetails() {
        return this.colDetails;
    }
    
    @Override
    public String createSQL() {
        return "create table chkpoint (id varchar(100) primary key, sourceposition blob, pendingddl bit(1), ddl longtext);";
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
