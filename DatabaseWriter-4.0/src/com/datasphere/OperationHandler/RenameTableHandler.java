package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import java.sql.*;

public class RenameTableHandler extends PassThroughDDLHandler
{
    public RenameTableHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    public void onDDLOperation(final String targetTable, final HDEvent event) throws DatabaseWriterException {
        this.callbackIntf.onRenameTable(targetTable, event);
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final TAlterTableStatement renameStmt = (TAlterTableStatement)this.sqlParser.sqlstatements.get(0);
            final TObjectName srcTable = renameStmt.getTableName();
            final TObjectName tgtTable = renameStmt.getAlterTableOptionList().getAlterTableOption(0).getNewTableName();
            final String sourceSchema = this.callbackIntf.getSchema();
            MappedName mappedName = this.getMappedName(catalog, schema, srcTable);
            srcTable.setString(mappedName.name);
            final String oldObjName = mappedName.toString();
            event.metadata.put("OldObjectName", oldObjName);
            mappedName = this.getMappedName(catalog, schema, tgtTable);
            tgtTable.setString(mappedName.name);
            final String newObjName = mappedName.toString();
            event.metadata.put("NewObjectName", newObjName);
            final String targetSchema = mappedName.schema;
            if (!targetSchema.equals(sourceSchema)) {
                final String tmpSQLCmd = ddlCmd = "begin \n execute immediate 'alter session set current_schema=" + targetSchema + "';\n execute immediate '" + ddlCmd + "';\n execute immediate 'alter session set current_schema=" + sourceSchema + "';\n end;\n";
            }
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
