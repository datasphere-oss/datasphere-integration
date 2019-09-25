package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;

import java.util.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import java.sql.*;

public class CreateViewHandler extends DDLHandler
{
    public CreateViewHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final Map<Long, String> tokenValues = new TreeMap<Long, String>();
            final TCreateViewSqlStatement createViewStmt = (TCreateViewSqlStatement)this.sqlParser.sqlstatements.get(0);
            final TObjectName viewName = createViewStmt.getViewName();
            final TSelectSqlStatement selectStmt = createViewStmt.getSubquery();
            final MappedName mappedViewName = this.getMappedName(catalog, schema, viewName);
            mappedViewName.update(viewName);
            final TTableList tables = selectStmt.tables;
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable t = tables.getTable(itr);
                final TObjectName oName = t.getTableName();
                final MappedName mappedTableName = this.getMappedName(catalog, schema, oName);
                mappedTableName.update(oName);
            }
            ddlCmd = createViewStmt.toString();
            return ddlCmd;
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
