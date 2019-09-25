package com.datasphere.intf;

import gudusoft.gsqlparser.*;

import java.sql.*;
import java.util.*;
import com.datasphere.source.cdc.common.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.Connection.*;
import com.datasphere.Table.SourceTableMap;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.TypeHandler.*;
import com.datasphere.databasewriter.*;
import com.datasphere.exception.*;
import com.datasphere.proc.events.*;

public interface DBInterface
{
    WildCardProcessor getWildcardProcessor();
    
    TGSqlParser getSQLParser();
    
    void onDDL(final EventData p0) throws SQLException, DatabaseWriterException;
    
    void onCreateTable(final String p0, final HDEvent p1) throws DatabaseWriterException;
    
    void onAlterTable(final String p0, final HDEvent p1) throws DatabaseWriterException;
    
    void onDropTable(final String p0, final HDEvent p1) throws DatabaseWriterException;
    
    void onRenameTable(final String p0, final HDEvent p1) throws DatabaseWriterException;
    
    int execute(final LinkedList<EventData> p0) throws SQLException, DatabaseWriterException;
    
    void commit() throws SQLException, DatabaseWriterException;
    
    void close() throws SQLException;
    
    DatabaseTable getTableMeta(final String p0) throws DatabaseWriterException;
    
    String getSchema() throws SQLException;
    
    DatabaseWriterConnection.TableNameParts getTableNameParts(final String p0);
    
    TableToTypeHandlerMap getTableToTypeHandlerMap();
    
    
    Connection getConnection();
    
    SourceTableMap getSourceTableMap();
    
    TargetTableMap getTargetTableMap();
    
    Property getProperty();
    
    void onDrop() throws DatabaseWriterException;
}
