package com.datasphere.Checkpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.recovery.Position;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.Table.Table;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;

public class BaseCheckpointTableImpl extends CheckpointTableImpl
{
    TargetTableMap tableMap;
    PreparedStatement chkPntUpdateStmt;
    boolean pendingDDL;
    String ddlCmd;
    int ddlType;
    private Logger logger;
    
    protected BaseCheckpointTableImpl(final String table, final String target, final DatabaseWriterConnection dbConnection) throws SQLException {
        super(table, target, dbConnection);
        this.pendingDDL = false;
        this.logger = LoggerFactory.getLogger((Class)BaseCheckpointTableImpl.class);
    }
    
    @Override
    public void initialize() throws DatabaseWriterException {
        try {
            this.tableMap = TargetTableMap.getInstance(this.targetId);
            final String query = "update " + this.tableName + " set sourceposition = ?, pendingddl = ?, ddl = ? where id = ?";
            this.chkPntUpdateStmt = this.connection.getConnection().prepareStatement(query);
        }
        catch (SQLException sExp) {
            throw new DatabaseWriterException("Got exception while initializing CheckpointTableImpl", sExp);
        }
    }
    
    @Override
    public void updateCheckPointTable(final Position position, final String ddl) throws DatabaseWriterException {
        try {
            final byte[] serializedPositionObj = KryoSingleton.write((Object)position, false);
            this.chkPntUpdateStmt.setBytes(1, serializedPositionObj);
            if (ddl != null && !ddl.isEmpty()) {
                this.chkPntUpdateStmt.setInt(2, 1);
                this.chkPntUpdateStmt.setString(3, ddl);
            }
            else {
                this.chkPntUpdateStmt.setInt(2, 0);
                this.chkPntUpdateStmt.setNull(3, this.ddlType);
            }
            this.chkPntUpdateStmt.setString(4, this.targetId);
            this.chkPntUpdateStmt.addBatch();
            this.chkPntUpdateStmt.executeBatch();
        }
        catch (SQLException exp) {
            final SQLException nextExp = exp.getNextException();
            if (nextExp != null) {
                final String expMsg = nextExp.getMessage();
                final String originalExpMsg = nextExp.getMessage();
                final String newExpMsg = String.valueOf(originalExpMsg) + "{" + expMsg + "}";
                final String sqlState = nextExp.getSQLState();
                final int vendorExceptionCode = nextExp.getErrorCode();
                exp = new SQLException(newExpMsg, sqlState, vendorExceptionCode);
            }
            throw new DatabaseWriterException("Exception while updating Checkpoint Table", exp);
        }
    }
    
    @Override
    public Position fetchPosition(final String target) throws DatabaseWriterException {
        Position currentChkPnt = null;
        boolean hasEntry = false;
        try {
            final String fetchQuery = "select sourceposition, pendingddl, ddl from " + this.tableName + " where id = ?";
            final PreparedStatement fetchStmt = this.connection.getConnection().prepareStatement(fetchQuery);
            fetchStmt.setString(1, this.targetId);
            final ResultSet rs = fetchStmt.executeQuery();
            while (rs.next()) {
                hasEntry = true;
                InputStream stream = rs.getBinaryStream(1);
                final byte[] positionData = streamToBytes(stream);
                if (positionData != null) {
                    currentChkPnt = (Position)KryoSingleton.read(positionData, false);
                }
                final int ddlPending = rs.getInt(2);
                if (ddlPending != 0) {
                    this.pendingDDL = true;
                }
                stream = rs.getAsciiStream(3);
                this.ddlCmd = streamToString(stream);
            }
            if (!hasEntry) {
                final String insertQuery = "insert into " + this.tableName + " (id, sourceposition) values (?, null)";
                final PreparedStatement insertStmt = this.connection.getConnection().prepareStatement(insertQuery);
                insertStmt.setString(1, target);
                insertStmt.execute();
                this.connection.getConnection().commit();
            }
        }
        catch (SQLException sExp) {
            final String errMsg = "Got exception while fetching checkpoint position {" + sExp.getMessage() + "}";
            throw new DatabaseWriterException(errMsg, sExp);
        }
        catch (IOException e) {
            throw new DatabaseWriterException("IOException while fetching position details");
        }
        return currentChkPnt;
    }
    
    @Override
    public boolean verifyCheckpointTable() throws DatabaseWriterException {
        final Map<String, ColumnDetails> columnDetails = this.getColumnDetails();
        this.ddlType = columnDetails.get("ddl").type;
        String errMsg = "";
        final String fqn = this.connection.fullyQualifiedName(this.tableName);
        try {
            this.tableMap.printMap();
            final Table targetTableDef = (Table)this.tableMap.getTableMeta(this.tableName);
            final DatabaseColumn[] columns = targetTableDef.getColumns();
            for (int itr = 0; itr < columns.length; ++itr) {
                final String colName = columns[itr].getName();
                final ColumnDetails colDetails = columnDetails.get(colName);
                if (colDetails != null) {
                    if (colDetails.isKey && !columns[itr].isKey()) {
                        errMsg = "Column {" + colName + "} is suppose to be a key field, but it is not in {" + fqn + "}. \nPlease specify the proper checkpoint table value using 'CheckPointTable' property or  Please use the below SQL for creating a new checkpoint table\n" + this.createSQL();
                    }
                    if (colDetails.type != columns[itr].getTypeCode()) {
                        System.out.println("colName\u5217\u540d\u79f0colDetails.type" + colDetails.type + "| columns[itr].getTypeCode()" + columns[itr].getTypeCode());
                        errMsg = "Checkpoint table {" + fqn + "} does not contain proper column type for {" + colName + "}. \nPlease specify the proper checkpoint table value using 'CheckPointTable' property or  Please use the below SQL for creating a new checkpoint table\n" + this.createSQL();
                    }
                    columnDetails.remove(colName);
                }
            }
            if (columnDetails.size() > 0) {
                String missingColumns = "";
                for (final String column : columnDetails.keySet()) {
                    if (missingColumns.isEmpty()) {
                        missingColumns = column;
                    }
                    else {
                        missingColumns = String.valueOf(missingColumns) + "," + column;
                    }
                }
                errMsg = "Missing {" + missingColumns + "} columns in {" + fqn + "} table. \nPlease specify the proper checkpoint table value using 'CheckPointTable' property or  Please use the below SQL for creating a new checkpoint table\n" + this.createSQL();
            }
        }
        catch (DatabaseWriterException exp) {
            if (exp.getErrorCode() == Error.TARGET_TABLE_DOESNOT_EXISTS.getType()) {
                exp.printStackTrace();
                final String errorMsg = "Checkpoint table {" + fqn + "} could not be located. \nPlease specify the proper checkpoint table value using 'CheckPointTable' property or  Please use the below SQL for creating a new checkpoint table\n" + this.createSQL();
                throw new DatabaseWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, errorMsg);
            }
        }
        if (!errMsg.isEmpty()) {
            throw new DatabaseWriterException(Error.INCORRECT_CHECKPOINT_TABLE_STRCUTRE, errMsg);
        }
        return true;
    }
    
    @Override
    public Map<String, ColumnDetails> getColumnDetails() {
        return null;
    }
    
    public static byte[] streamToBytes(final InputStream stream) throws IOException {
        if (stream != null) {
            final byte[] buffer = new byte[4096];
            int bytesRead = 0;
            final ByteArrayOutputStream outputBinStream = new ByteArrayOutputStream();
            while ((bytesRead = stream.read(buffer, 0, buffer.length)) > 0) {
                outputBinStream.write(buffer, 0, bytesRead);
            }
            return outputBinStream.toByteArray();
        }
        return null;
    }
    
    public static String streamToString(final InputStream stream) throws IOException {
        if (stream != null) {
            final byte[] byteData = streamToBytes(stream);
            return new String(byteData, StandardCharsets.UTF_8);
        }
        return null;
    }
    
    @Override
    public boolean pendingDDL() {
        return this.pendingDDL;
    }
    
    @Override
    public String ddl() {
        return this.ddlCmd;
    }
    
    @Override
    public String createSQL() {
        return null;
    }
    
    class SecurityAccess
    {
        public void disopen() {
        }
    }
}
