package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import java.sql.*;

public class CreateSequenceHandler extends DDLHandler
{
    public CreateSequenceHandler(final DBInterface intf) {
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
            final TCreateSequenceStmt createSeq = (TCreateSequenceStmt)this.sqlParser.sqlstatements.get(0);
            final TObjectName indexName = createSeq.getSequenceName();
            final MappedName mappedName = this.getMappedName(catalog, schema, indexName);
            mappedName.update(indexName);
            ddlCmd = createSeq.toString();
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
