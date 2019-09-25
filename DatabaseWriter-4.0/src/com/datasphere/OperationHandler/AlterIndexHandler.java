package com.datasphere.OperationHandler;

import gudusoft.gsqlparser.*;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import java.sql.*;
import java.util.*;

public class AlterIndexHandler extends CreateIndexHandler
{
    private static TreeMap<String, String> tokenToIgnore;
    String schemaToUse;
    
    public AlterIndexHandler(final DBInterface intf) {
        super(intf);
    }
    
    private String manuallyParseStmt(final String catalog, final String schema, final TCustomSqlStatement stmt) {
        final StringBuilder query = new StringBuilder();
        String nameToTransform = "";
        final TSourceTokenList lst = stmt.sourcetokenlist;
        for (int itr = 0; itr < lst.size(); ++itr) {
            final TSourceToken tkn = lst.get(itr);
            final String tknStr = tkn.toString();
            if (!AlterIndexHandler.tokenToIgnore.containsKey(tknStr) && (tkn.tokentype == ETokenType.ttidentifier || tknStr.equals("."))) {
                nameToTransform += tknStr;
            }
            else {
                if (!nameToTransform.isEmpty()) {
                    final MappedName mappedName = this.getMappedName(catalog, schema, nameToTransform);
                    if (nameToTransform.contains(".")) {
                        query.append(mappedName.toString());
                    }
                    else {
                        query.append(mappedName.name);
                    }
                    this.schemaToUse = mappedName.schema;
                    nameToTransform = "";
                }
                query.append(tknStr);
            }
        }
        if (!nameToTransform.isEmpty()) {
            final MappedName mappedName = this.getMappedName(catalog, schema, nameToTransform);
            if (nameToTransform.contains(".")) {
                query.append(mappedName.toString());
            }
            else {
                query.append(mappedName.name);
            }
        }
        return query.toString();
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
            final String sourceSchema = this.callbackIntf.getSchema();
            final TCustomSqlStatement customStmt = this.sqlParser.sqlstatements.get(0);
            if (customStmt instanceof TAlterIndexStmt) {
                final TAlterIndexStmt alterIndex = (TAlterIndexStmt)this.sqlParser.sqlstatements.get(0);
                final TObjectName indexName = alterIndex.getIndexName();
                mappedName = this.getMappedName(catalog, schema, indexName);
                mappedName.update(indexName);
                ddlCmd = customStmt.toString();
            }
            else {
                ddlCmd = this.manuallyParseStmt(catalog, schema, customStmt);
            }
            if (!this.schemaToUse.equals(sourceSchema)) {
                final String tmpSQLCmd = ddlCmd = "begin \n execute immediate 'alter session set current_schema=" + this.schemaToUse + "';\n execute immediate '" + ddlCmd + "';\n execute immediate 'alter session set current_schema=" + sourceSchema + "';\n end;\n";
            }
            return ddlCmd;
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    static {
        AlterIndexHandler.tokenToIgnore = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER) {
            {
                this.put("rebuild", "rebuild");
            }
        };
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
