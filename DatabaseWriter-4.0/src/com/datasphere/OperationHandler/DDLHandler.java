package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TObjectName;

public class DDLHandler extends OperationHandler
{
    Map<String, MappedName> sourceTargetMapCache;
    private Logger logger;
    TGSqlParser sqlParser;
    
    public DDLHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)DDLHandler.class);
        this.sqlParser = intf.getSQLParser();
        this.sourceTargetMapCache = new HashMap<String, MappedName>();
    }
    
    @Override
    public void validate(final HDEvent event) throws DatabaseWriterException {
        if (event.data == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATA, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
        if (event.dataPresenceBitMap == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATAPRESENCEBITMAP, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
        if (event.metadata.get(Constant.OBJECT_NAME) == null) {
            throw new DatabaseWriterException("Event metadata did not have " + Constant.OBJECT_NAME);
        }
    }
    
    @Override
    public void bind(final String targetTable, final HDEvent event, final PreparedStatement stmt) throws SQLException, DatabaseWriterException {
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        return (String)event.data[0];
    }
    
    @Override
    public String generateDML(final HDEvent event, final String targetTable) throws SQLException {
        return null;
    }
    
    @Override
    public String getComponentName(final HDEvent event) {
        final String catObjName = (String)event.metadata.get(Constant.OBJECT_NAME);
        if (catObjName.indexOf(46) != -1) {
            return catObjName;
        }
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        String fullyQualifiedName = catalog;
        if (schema != null && !schema.isEmpty()) {
            fullyQualifiedName = ((fullyQualifiedName != null && !fullyQualifiedName.isEmpty()) ? (fullyQualifiedName + "." + schema) : schema);
        }
        fullyQualifiedName = ((fullyQualifiedName != null && !fullyQualifiedName.isEmpty()) ? (fullyQualifiedName + "." + catObjName) : catObjName);
        return fullyQualifiedName;
    }
    
    protected MappedName getMappedName(final String catalog, final String _schema, final TObjectName table) {
        final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(table.toString());
        String ctlg = catalog;
        if (tps.catalog != null && !tps.catalog.isEmpty()) {
            ctlg = tps.catalog;
        }
        String schema = _schema;
        if (tps.schema != null && !tps.schema.isEmpty()) {
            schema = tps.schema;
        }
        if (ctlg == null || !ctlg.isEmpty()) {
            return this.getMappedName(schema, tps.table);
        }
        return this.getMappedName(ctlg, schema, tps.table);
    }
    
    protected MappedName getMappedName(String database, String schema, final String table) {
        String name = null;
        final String[] parts = table.split("\\.");
        if (parts.length < 2) {
            name = parts[0];
        }
        else if (parts.length < 3) {
            schema = parts[0];
            name = parts[1];
        }
        else {
            database = parts[0];
            schema = parts[1];
            name = parts[2];
        }
        MappedName mappedName = null;
        if (database == null || !database.isEmpty()) {
            mappedName = this.getMappedName(schema, name);
        }
        else {
            mappedName = this.getMappedName(database + "." + schema + "." + name);
        }
        if (mappedName == null) {
            mappedName = new MappedName(database, schema, name);
        }
        return mappedName;
    }
    
    protected MappedName getMappedName(final String schema, final String table) {
        MappedName mappedName = this.getMappedName(schema + "." + table);
        if (mappedName == null) {
            mappedName = new MappedName(null, schema, table);
        }
        return mappedName;
    }
    
    protected MappedName getMappedName(final String sourceName) {
        MappedName target = this.sourceTargetMapCache.get(sourceName);
        if (target == null) {
            final ArrayList<String> mappedTargetNames = this.callbackIntf.getWildcardProcessor().getMapForSourceTable(sourceName);
            if (mappedTargetNames == null || mappedTargetNames.isEmpty()) {
                return target;
            }
            if (mappedTargetNames.size() > 1) {
                final MappedName srcName = new MappedName(sourceName);
                for (int itr = 0; itr < mappedTargetNames.size(); ++itr) {
                    final MappedName tgtName = new MappedName(mappedTargetNames.get(itr));
                    if (srcName.isSame(tgtName)) {
                        target = tgtName;
                        break;
                    }
                }
            }
            else {
                target = new MappedName(mappedTargetNames.get(0));
            }
            this.sourceTargetMapCache.put(sourceName, target);
        }
        return target;
    }
    
    @Override
    public boolean isDDLOperation() {
        return true;
    }
    
    class MappedName
    {
        public String name;
        public String schema;
        public String catalog;
        
        public MappedName(final String catalog, final String schema, final String name) {
            this.catalog = catalog;
            this.schema = schema;
            this.name = name;
        }
        
        public MappedName(final String fullyQualifiedName) {
            final String[] parts = fullyQualifiedName.split("\\.");
            if (parts.length < 2) {
                this.name = parts[0];
            }
            else if (parts.length < 3) {
                this.schema = parts[0];
                this.name = parts[1];
            }
            else {
                this.catalog = parts[0];
                this.schema = parts[1];
                this.name = parts[2];
            }
        }
        
        public boolean isSame(final MappedName obj) {
            return this.catalog != null && this.catalog.equals(obj.catalog) && this.schema != null && this.schema.equals(obj.schema);
        }
        
        @Override
        public String toString() {
            final StringBuilder retValue = new StringBuilder();
            if (this.catalog != null) {
                retValue.append(this.catalog);
            }
            if (this.schema != null) {
                if (retValue.length() == 0) {
                    retValue.append(this.schema);
                }
                else {
                    retValue.append(".");
                    retValue.append(this.schema);
                }
            }
            if (retValue.length() == 0) {
                retValue.append(this.name);
            }
            else {
                retValue.append(".");
                retValue.append(this.name);
            }
            return retValue.toString();
        }
        
        public void update(final TObjectName tObject) {
            String text = "";
            if (this.catalog != null) {
                if (tObject.getDatabaseToken() != null) {
                    tObject.getDatabaseToken().astext = this.catalog;
                }
                else {
                    text = this.catalog;
                }
            }
            if (this.schema != null) {
                if (tObject.getSchemaToken() != null) {
                    if (text.isEmpty()) {
                        tObject.getSchemaToken().astext = this.schema;
                    }
                    else {
                        tObject.getSchemaToken().astext = text + "." + this.schema;
                        text = "";
                    }
                }
                else if (text.isEmpty()) {
                    text = this.schema;
                }
                else {
                    text = text + "." + this.schema;
                }
            }
            if (text.isEmpty()) {
                tObject.getTableToken().astext = this.name;
            }
            else {
                tObject.getTableToken().astext = text + "." + this.name;
            }
        }
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}
