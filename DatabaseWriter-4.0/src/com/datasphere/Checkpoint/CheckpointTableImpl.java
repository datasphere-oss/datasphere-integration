package com.datasphere.Checkpoint;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.recovery.Position;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.Utils;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.exception.DatabaseWriterException;

public abstract class CheckpointTableImpl
{
    DatabaseWriterConnection connection;
    String targetId;
    String tableName;
    private static Logger logger;
    
    protected CheckpointTableImpl(final String table, final String target, final DatabaseWriterConnection dbConnection) {
        this.connection = dbConnection;
        this.targetId = target;
        this.tableName = table;
    }
    
    public abstract void initialize() throws DatabaseWriterException;
    
    public abstract void updateCheckPointTable(final Position p0, final String p1) throws DatabaseWriterException;
    
    public abstract Position fetchPosition(final String p0) throws DatabaseWriterException;
    
    public abstract boolean verifyCheckpointTable() throws DatabaseWriterException;
    
    public abstract Map<String, ColumnDetails> getColumnDetails();
    
    public abstract boolean pendingDDL();
    
    public abstract String ddl();
    
    public abstract String createSQL();
    
    public static CheckpointTableImpl getInstance(final Property prop, final DatabaseWriterConnection connection) throws DatabaseWriterException {
        CheckpointTableImpl impl = null;
        final String url = prop.getString(Constant.CONNECTION_URL, (String)null);
        if (url == null) {
            throw new DatabaseWriterException("NULL Connection URL passed");
        }
        final String targetType = Utils.getDBType(url);
        final String targetId = prop.getString(Constant.TARGET_UUID, (String)null);
        final String tableName = prop.getString(Constant.CHECK_POINT_TABLE, (String)null);
        try {
            if (Constant.ORACLE_TYPE.equalsIgnoreCase(targetType)) {
                impl = new OracleCheckpointTableImpl(tableName, targetId, connection);
            }
            else if (Constant.MYSQL_TYPE.equalsIgnoreCase(targetType)) {
                impl = new MySQLCheckpointTableImpl(tableName, targetId, connection);
            }
            else if (Constant.MSSQL_TYPE.equalsIgnoreCase(targetType)) {
                impl = new MSSQLCheckpointTableImpl(tableName, targetId, connection);
            }
            else if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(targetType) || Constant.EDB_TYPE.equalsIgnoreCase(targetType)) {
                impl = new PostgreSQLCheckpointTableImpl(tableName, targetId, connection);
            }
            else {
                CheckpointTableImpl.logger.warn("E1P is not supported for {" + targetType + "}, disabling Checkpoint Table updation");
                impl = new EmptyCheckpointTableImpl(tableName, targetId, connection);
            }
        }
        catch (Exception exp) {
            throw new DatabaseWriterException("Exception while instantiating CheckpointTable implementation", exp);
        }
        return impl;
    }
    
    static {
        CheckpointTableImpl.logger = LoggerFactory.getLogger(CheckpointTableImpl.class);
    }
    
    class ColumnDetails
    {
        public String name;
        public boolean isKey;
        public int type;
        
        public ColumnDetails(final String name, final int type, final boolean isKey) {
            this.name = name;
            this.type = type;
            this.isKey = isKey;
        }
    }
}
