package com.datasphere.OperationHandler;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.stmt.TAlterTableStatement;

public class AlterTableHandler extends DDLHandler
{
    private Logger logger;
    
    public AlterTableHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)AlterTableHandler.class);
    }
    
    @Override
    public void onDDLOperation(final String targetTable, final HDEvent event) throws DatabaseWriterException {
        this.callbackIntf.onAlterTable(targetTable, event);
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final TAlterTableStatement alterStmt = (TAlterTableStatement)this.sqlParser.sqlstatements.get(0);
            final TTableList tables = alterStmt.tables;
            for (int itr = 0; itr < tables.size(); ++itr) {
                final TTable t = tables.getTable(itr);
                final TObjectName oName = t.getTableName();
                final MappedName mappedName = this.getMappedName(catalog, schema, oName);
                mappedName.update(oName);
            }
            ddlCmd = alterStmt.toString();
            return ddlCmd;
        }
        if (ddlCmd.toUpperCase().contains(" PARTITION")) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Manullay parsing sql with partition");
            }
            final String[] sqlParts = ddlCmd.split("\\s+");
            if (sqlParts[1].equalsIgnoreCase("TABLE")) {
                final MappedName mappedName2 = this.getMappedName(catalog, schema, sqlParts[2]);
                sqlParts[2] = mappedName2.toString();
            }
            final StringBuilder cmdBldr = new StringBuilder();
            for (int itr = 0; itr < sqlParts.length; ++itr) {
                cmdBldr.append(sqlParts[itr]);
                cmdBldr.append(" ");
            }
            return cmdBldr.toString();
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
