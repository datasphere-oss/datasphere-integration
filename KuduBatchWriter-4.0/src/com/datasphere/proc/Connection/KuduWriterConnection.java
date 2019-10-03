package com.datasphere.proc.Connection;

import com.datasphere.source.lib.prop.*;
import com.datasphere.proc.exception.*;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.source.cdc.common.*;

import org.apache.kudu.*;
import com.datasphere.Tables.*;
import org.apache.kudu.client.*;

public class KuduWriterConnection
{
    protected KuduClient.KuduClientBuilder builder;
    protected KuduClient client;
    protected KuduSession connection;
    protected Property prop;
    private long executionLatency;
    private long totalRows;
    private String operationMetrics;
    private String lastIOTime;
    private String positionString;
    protected Map<String, KuduTable> tableInstance;
    private Logger logger;
    
    public KuduWriterConnection(final Property prop) {
        this.logger = Logger.getLogger((Class)KuduWriterConnection.class);
        this.prop = prop;
        this.connection = null;
        this.connect();
        this.executionLatency = 0L;
        this.totalRows = 0L;
        this.operationMetrics = new String();
        this.lastIOTime = new String();
        this.positionString = new String();
        this.tableInstance = new HashMap<String, KuduTable>();
    }
    
    public String targetType() {
        return "kudu";
    }
    
    public void connect() {
        final List<String> kuduMasterAddresses = (List<String>)this.prop.getObject("kuduMasterAddresses", (Object)null);
        final long socketreadtimeout = (long)this.prop.getObject("socketreadtimeout", (Object)null);
        final long operationtimeout = (long)this.prop.getObject("operationtimeout", (Object)null);
        (this.builder = new KuduClient.KuduClientBuilder((List)kuduMasterAddresses)).defaultSocketReadTimeoutMs(socketreadtimeout);
        this.builder.defaultOperationTimeoutMs(operationtimeout);
        this.client = this.builder.build();
        this.connection = this.client.newSession();
    }
    
    public KuduSession getConnection() {
        return this.connection;
    }
    
    public KuduClient getClient() {
        return this.client;
    }
    
    public static KuduWriterConnection getConnection(final Property prop) {
        KuduWriterConnection connection = null;
        connection = new KuduWriterConnection(prop);
        return connection;
    }
    
    public DatabaseTable getTableMeta(final String tableName) throws KuduWriterException {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)("Loading table metadata for {" + tableName + "}"));
        }
        KuduTable tab = this.tableInstance.get(tableName);
        if (tab == null) {
            try {
                if (!this.client.tableExists(tableName)) {
                    throw new KuduWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, "Got exception while fetching table metadata of {" + tableName + "}");
                }
                try {
                    tab = this.client.openTable(tableName);
                }
                catch (KuduException e) {
                    throw new KuduWriterException(e.getStatus().toString());
                }
                this.tableInstance.put(tableName, tab);
            }
            catch (KuduException e) {
                throw new KuduWriterException(e.getStatus().toString());
            }
        }
        final Schema sch = tab.getSchema();
        final List<ColumnSchema> allSchema = (List<ColumnSchema>)sch.getColumns();
        try {
            final Table dbTable = this.createTable(tableName, allSchema);
            return (DatabaseTable)dbTable;
        }
        catch (Exception sExp) {
            throw new KuduWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, "Got exception while fetching table metadata of {" + tableName + "}", sExp);
        }
    }
    
    public Table createTable(final String tableName, final List<ColumnSchema> allSchema) {
        return new Table(tableName, (List)allSchema);
    }
    
    public KuduTable getTableInstance(final String tableName) throws KuduWriterException {
        KuduTable tab = this.tableInstance.get(tableName);
        if (tab == null) {
            try {
                if (!this.client.tableExists(tableName)) {
                    throw new KuduWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, "Got exception while fetching table metadata of {" + tableName + "}");
                }
                tab = this.client.openTable(tableName);
                this.tableInstance.put(tableName, tab);
            }
            catch (KuduException e) {
                throw new KuduWriterException(e.getStatus().toString());
            }
        }
        return tab;
    }
    
    public void setSessionProperty(final Property prop2) {
        this.connection.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        this.connection.setMutationBufferSpace(prop2.getInt("EventCount", 1000));
        this.connection.setMutationBufferLowWatermark(1.0f);
    }
    
    public void setLatency(final long executionLatency) {
        this.executionLatency = executionLatency;
    }
    
    public void setTotalEvent(final long totalRows) {
        this.totalRows = totalRows;
    }
    
    public long getLatency() {
        return this.executionLatency;
    }
    
    public long totalEventCount() {
        return this.totalRows;
    }
    
    public void setOperationMetrics(final String operationMetrics) {
        this.operationMetrics = operationMetrics;
    }
    
    public String getOperationMetrics() {
        return this.operationMetrics;
    }
    
    public void setlastIOTime(final String lastIOTime) {
        this.lastIOTime = lastIOTime;
    }
    
    public String getLastIOTime() {
        return this.lastIOTime;
    }
    
    public void setAcknowledgedCheckpoint(final String positionString) {
        this.positionString = positionString;
    }
    
    public String getAcknowledgedCheckpoint() {
        return this.positionString;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
