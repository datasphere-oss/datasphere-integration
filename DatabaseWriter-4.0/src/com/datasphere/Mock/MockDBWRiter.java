package com.datasphere.Mock;

import com.datasphere.source.lib.prop.*;
import com.datasphere.Connection.*;
import com.datasphere.databasewriter.*;
import com.datasphere.exception.*;
import com.datasphere.proc.events.*;

import gudusoft.gsqlparser.*;

import java.sql.*;
import java.util.*;
import com.datasphere.source.cdc.common.*;

public class MockDBWRiter extends DatabaseWriterProcessEvent
{
    WildCardProcessor wCardProcessor;
    
    public MockDBWRiter(final DatabaseWriterConnection connectionObject, final WildCardProcessor wildcardProcessor, final Property prop) throws DatabaseWriterException {
        super(connectionObject, wildcardProcessor, prop);
    }
    
    @Override
    protected void init(final Property prop) throws DatabaseWriterException {
        this.wCardProcessor = new WildCardProcessor();
        final String[] tables = { "DDLTEST.%,DDLTEST.%" };
        try {
            this.wCardProcessor.initializeWildCardProcessor(tables, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public WildCardProcessor getWildcardProcessor() {
        return this.wCardProcessor;
    }
    
    @Override
    public TGSqlParser getSQLParser() {
        return new TGSqlParser(EDbVendor.dbvoracle);
    }
    
    @Override
    public void onDDL(final EventData ddlEvent) throws SQLException, DatabaseWriterException {
    }
    
    @Override
    public void onCreateTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
    }
    
    @Override
    public void onAlterTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
    }
    
    @Override
    public void onDropTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
    }
    
    @Override
    public void onRenameTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
    }
    
    @Override
    public int execute(final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        return 0;
    }
    
    @Override
    public void commit() throws SQLException, DatabaseWriterException {
    }
    
    @Override
    public void close() throws SQLException {
    }
    
    @Override
    public DatabaseTable getTableMeta(final String tableName) throws DatabaseWriterException {
        return null;
    }
    
    @Override
    public String getSchema() throws SQLException {
        return "DDLTEST";
    }
    
    public class TableNameParts
    {
        public String schema;
        public String catalog;
        public String table;
        
        public TableNameParts(final String catalog, final String schema, final String table) {
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
        }
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
