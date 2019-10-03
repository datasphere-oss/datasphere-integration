package com.datasphere.proc.CheckPoint;

import com.datasphere.utils.writers.common.*;
import com.datasphere.proc.Connection.*;
import com.datasphere.proc.exception.*;
import com.datasphere.ser.*;
import com.datasphere.recovery.*;
import org.apache.kudu.*;
import java.util.*;

import org.apache.kudu.client.*;

public class CheckpointTableImpl implements Checkpointer
{
    private String tableName;
    private String componentName;
    private KuduWriterConnection connectionObject;
    private PathManager checkpointingValue;
    
    public CheckpointTableImpl(final String tableName, final String componentName, final KuduWriterConnection connectionObject) throws KuduWriterException {
        this.checkpointingValue = new PathManager();
        this.tableName = tableName;
        this.componentName = componentName;
        this.connectionObject = connectionObject;
        this.checkForCheckpointTable();
    }
    
    public void updatePositionToDb() throws KuduWriterException {
        final KuduClient client = this.connectionObject.getClient();
        final KuduSession sess = client.newSession();
        final Position pos = this.checkpointingValue.toPosition();
        final KuduTable tab = this.connectionObject.getTableInstance(this.tableName);
        final byte[] serializedPositionObj = KryoSingleton.write((Object)pos, false);
        final Upsert upsert = tab.newUpsert();
        final PartialRow row = upsert.getRow();
        row.addString("ID", this.componentName);
        if (serializedPositionObj != null) {
            row.addBinary("POSITION", serializedPositionObj);
        }
        else {
            row.setNull("POSITION");
        }
        try {
            final OperationResponse response = sess.apply((Operation)upsert);
            if (response.hasRowError()) {
                throw new KuduWriterException("Couldnot persist row : " + response.getRowError().getOperation().getRow() + " : " + response.getRowError().toString());
            }
            sess.close();
        }
        catch (KuduException e) {
            throw new KuduWriterException(e.getStatus().toString());
        }
    }
    
    public Position getAckPosition() throws KuduWriterException {
        Position currentChkPnt = null;
        final KuduTable tab = this.connectionObject.getTableInstance(this.tableName);
        final KuduClient client = this.connectionObject.getClient();
        final KuduScanner.KuduScannerBuilder sca = client.newScannerBuilder(tab);
        RowResult result = null;
        sca.addPredicate(KuduPredicate.newComparisonPredicate(tab.getSchema().getColumn("ID"), KuduPredicate.ComparisonOp.EQUAL, this.componentName));
        final KuduScanner scanfinal = sca.build();
        RowResultIterator iterator = null;
        try {
            iterator = scanfinal.nextRows();
        }
        catch (KuduException e) {
            throw new KuduWriterException(e.getStatus().toString());
        }
        if (iterator.hasNext()) {
            result = iterator.next();
            final byte[] positionData = result.getBinaryCopy("POSITION");
            currentChkPnt = (Position)KryoSingleton.read(positionData, false);
        }
        return currentChkPnt;
    }
    
    private void checkForCheckpointTable() throws KuduWriterException {
        final KuduClient client = this.connectionObject.getClient();
        try {
            if (!client.tableExists(this.tableName)) {
                final List<String> keysColumn = new ArrayList<String>();
                keysColumn.add("ID");
                final List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
                for (int i = 0; i < 2; ++i) {
                    ColumnSchema col;
                    if (i == 0) {
                        col = new ColumnSchema.ColumnSchemaBuilder("ID", Type.STRING).key(true).build();
                    }
                    else {
                        col = new ColumnSchema.ColumnSchemaBuilder("POSITION", Type.BINARY).key(false).nullable(true).build();
                    }
                    columns.add(col);
                }
                final Schema schema = new Schema((List)columns);
                final CreateTableOptions options = new CreateTableOptions();
                options.setNumReplicas(1);
                options.setRangePartitionColumns((List)keysColumn);
                client.createTable(this.tableName, schema, options);
            }
            else {
                final KuduTable tab = this.connectionObject.getTableInstance(this.tableName);
                final Schema sch = tab.getSchema();
                for (int index = 0; index < 2; ++index) {
                    final String colType = sch.getColumnByIndex(index).getType().getDataType().toString().toLowerCase();
                    final String colName = sch.getColumnByIndex(index).getName();
                    if (index != 0 || !colType.equals("string") || !colName.equals("ID")) {
                        if (index != 1 || !colType.equals("binary") || !colName.equals("POSITION")) {
                            throw new KuduWriterException(Error.INCORRECT_CHECKPOINT_TABLE_STRCUTRE, "create a kudu table with 2 columns with 1st column name ID as primary key of string type and 2nd column name POSITION of binary type");
                        }
                    }
                }
            }
        }
        catch (KuduException e) {
            throw new KuduWriterException(e.getStatus().toString());
        }
    }
    
    public void updatePosition(final Position pos) {
        this.checkpointingValue.mergeHigherPositions(pos);
    }
    
    public void deletePositionFromDb() throws KuduWriterException {
        final KuduClient client = this.connectionObject.getClient();
        final KuduSession sess = client.newSession();
        final KuduTable tab = this.connectionObject.getTableInstance(this.tableName);
        final KuduScanner.KuduScannerBuilder sca = client.newScannerBuilder(tab);
        sca.addPredicate(KuduPredicate.newComparisonPredicate(tab.getSchema().getColumn("ID"), KuduPredicate.ComparisonOp.EQUAL, this.componentName));
        final KuduScanner scanfinal = sca.build();
        RowResultIterator iterator = null;
        try {
            iterator = scanfinal.nextRows();
        }
        catch (KuduException e1) {
            throw new KuduWriterException(e1.getMessage());
        }
        final int numOfRows = iterator.getNumRows();
        if (numOfRows == 0) {
            return;
        }
        final Delete delete = tab.newDelete();
        final PartialRow row = delete.getRow();
        row.addString("ID", this.componentName);
        try {
            final OperationResponse response = sess.apply((Operation)delete);
            if (response.hasRowError()) {
                throw new KuduWriterException("Couldnot persist row :" + response.getRowError().toString());
            }
            sess.close();
        }
        catch (KuduException e2) {
            throw new KuduWriterException(e2.getStatus().toString());
        }
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
