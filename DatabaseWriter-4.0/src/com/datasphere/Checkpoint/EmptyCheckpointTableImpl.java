package com.datasphere.Checkpoint;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.recovery.Position;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.exception.DatabaseWriterException;

public class EmptyCheckpointTableImpl extends CheckpointTableImpl
{
    private Logger logger;
    
    protected EmptyCheckpointTableImpl(final String table, final String target, final DatabaseWriterConnection dbConnection) {
        super(table, target, dbConnection);
        (this.logger = LoggerFactory.getLogger((Class)EmptyCheckpointTableImpl.class)).warn("E1P - Checkpoint Table update is disabled");
    }
    
    @Override
    public Position fetchPosition(final String target) throws DatabaseWriterException {
        return null;
    }
    
    @Override
    public boolean verifyCheckpointTable() throws DatabaseWriterException {
        return true;
    }
    
    @Override
    public Map<String, ColumnDetails> getColumnDetails() {
        return null;
    }
    
    @Override
    public boolean pendingDDL() {
        return false;
    }
    
    @Override
    public String ddl() {
        return null;
    }
    
    @Override
    public String createSQL() {
        return null;
    }
    
    @Override
    public void updateCheckPointTable(final Position position, final String ddl) throws DatabaseWriterException {
    }
    
    @Override
    public void initialize() throws DatabaseWriterException {
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
