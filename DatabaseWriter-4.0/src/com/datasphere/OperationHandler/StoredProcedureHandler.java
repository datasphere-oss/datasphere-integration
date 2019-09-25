package com.datasphere.OperationHandler;

import com.datasphere.Connection.*;
import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;

import java.sql.*;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.*;
import java.util.*;

public abstract class StoredProcedureHandler extends DDLHandler
{
    protected static Map<String, String> ignoreableTableNames;
    
    public StoredProcedureHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        MappedName mappedName = null;
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final TCommonStoredProcedureSqlStatement commStoredProcStmt = (TCommonStoredProcedureSqlStatement)this.sqlParser.sqlstatements.get(0);
            final TObjectName spName = this.getStoredProcedureName(commStoredProcStmt);
            mappedName = this.getMappedName(catalog, schema, spName);
            mappedName.update(spName);
            final TStatementList stmtLst = commStoredProcStmt.getStatements();
            for (int stmtItr = 0; stmtItr < stmtLst.size(); ++stmtItr) {
                final TCustomSqlStatement stmt = stmtLst.get(stmtItr);
                this.updateStatement(catalog, schema, stmt);
            }
            ddlCmd = commStoredProcStmt.toString();
            return ddlCmd;
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    private void updateStatement(final String catalog, final String schema, final TInsertSqlStatement stmt) {
        final TTableList tables = stmt.tables;
        if (tables.size() > 0) {
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable table = tables.getTable(itr);
                if (table.subquery != null) {
                    this.updateStatement(catalog, schema, table.subquery);
                }
                else {
                    final TObjectName tblName = table.getTableName();
                    final String tableName = tblName.toString();
                    if (!StoredProcedureHandler.ignoreableTableNames.containsKey(tableName)) {
                        final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(tableName);
                        final MappedName mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
                        mappedName.update(tblName);
                    }
                }
            }
        }
    }
    
    private void updateStatement(final String catalog, final String schema, final TUpdateSqlStatement stmt) {
        final TTableList tables = stmt.tables;
        if (tables.size() > 0) {
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable table = tables.getTable(itr);
                if (table.subquery != null) {
                    this.updateStatement(catalog, schema, table.subquery);
                }
                else {
                    final TObjectName tblName = table.getTableName();
                    final String tableName = tblName.toString();
                    if (!StoredProcedureHandler.ignoreableTableNames.containsKey(tableName)) {
                        final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(tableName);
                        final MappedName mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
                        mappedName.update(tblName);
                    }
                }
            }
        }
    }
    
    private void updateStatement(final String catalog, final String schema, final TDeleteSqlStatement stmt) {
        final TTableList tables = stmt.tables;
        if (tables.size() > 0) {
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable table = tables.getTable(itr);
                if (table.subquery != null) {
                    this.updateStatement(catalog, schema, table.subquery);
                }
                else {
                    final TObjectName tblName = table.getTableName();
                    final String tableName = tblName.toString();
                    if (!StoredProcedureHandler.ignoreableTableNames.containsKey(tableName)) {
                        final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(tableName);
                        final MappedName mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
                        mappedName.update(tblName);
                    }
                }
            }
        }
    }
    
    private void updateStatement(final String catalog, final String schema, final TSelectSqlStatement stmt) {
        final TTableList tables = stmt.tables;
        if (tables.size() > 0) {
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable table = tables.getTable(itr);
                if (table.subquery != null) {
                    this.updateStatement(catalog, schema, table.subquery);
                }
                else {
                    final TObjectName tblName = table.getTableName();
                    final String tableName = tblName.toString();
                    if (!StoredProcedureHandler.ignoreableTableNames.containsKey(tableName)) {
                        final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(tableName);
                        MappedName mappedName = this.getMappedName(catalog, schema, tblName);
                        mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
                        mappedName.update(tblName);
                    }
                }
            }
        }
    }
    
    protected void updateStatement(final String catalog, final String schema, final TCustomSqlStatement statement) {
        if (statement instanceof TSelectSqlStatement) {
            this.updateStatement(catalog, schema, (TSelectSqlStatement)statement);
        }
        else if (statement instanceof TInsertSqlStatement) {
            this.updateStatement(catalog, schema, (TInsertSqlStatement)statement);
        }
        else if (statement instanceof TUpdateSqlStatement) {
            this.updateStatement(catalog, schema, (TUpdateSqlStatement)statement);
        }
        else if (statement instanceof TDeleteSqlStatement) {
            this.updateStatement(catalog, schema, (TDeleteSqlStatement)statement);
        }
        else {
            final TSourceTokenList tokenList = statement.sourcetokenlist;
            TSourceToken tokenToChange = null;
            String objectName = "";
            for (int itr = 0; itr < tokenList.size(); ++itr) {
                final TSourceToken token = tokenList.get(itr);
                final String tknStr = token.toString();
                if (token.tokentype == ETokenType.ttidentifier || tknStr.equals(".")) {
                    if (tokenToChange == null) {
                        tokenToChange = token;
                    }
                    objectName += tknStr;
                }
                else if (tknStr.trim().isEmpty() && tokenToChange != null) {
                    final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(objectName);
                    final MappedName mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
                    tokenToChange.setString(mappedName.toString());
                    objectName = "";
                    tokenToChange = null;
                }
            }
        }
    }
    
    protected abstract TObjectName getStoredProcedureName(final TCommonStoredProcedureSqlStatement p0);
    
    static {
        StoredProcedureHandler.ignoreableTableNames = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER) {
            private static final long serialVersionUID = 1L;
            
            {
                this.put("DUAL", "DUAL");
            }
        };
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
