package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.nodes.*;
import java.sql.*;

public class CreateIndexHandler extends PassThroughDDLHandler
{
    public CreateIndexHandler(final DBInterface intf) {
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
            final TCreateIndexSqlStatement createIdx = (TCreateIndexSqlStatement)this.sqlParser.sqlstatements.get(0);
            final TObjectName indexName = createIdx.getIndexName();
            final TObjectName tableName = createIdx.getTableName();
            MappedName mappedName = this.getMappedName(catalog, schema, indexName);
            mappedName.update(indexName);
            mappedName = this.getMappedName(catalog, schema, tableName);
            mappedName.update(tableName);
            ddlCmd = createIdx.toString();
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
