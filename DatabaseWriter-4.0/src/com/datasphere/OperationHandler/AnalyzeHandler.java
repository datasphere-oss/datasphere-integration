package com.datasphere.OperationHandler;

import com.datasphere.Connection.*;
import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;

import gudusoft.gsqlparser.*;

import java.sql.*;

public class AnalyzeHandler extends DDLHandler
{
    public AnalyzeHandler(final DBInterface intf) {
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
            final TCustomSqlStatement custStmt = this.sqlParser.sqlstatements.get(0);
            final TSourceTokenList tokenList = custStmt.sourcetokenlist;
            String tableName = "";
            TSourceToken tokenToReplace = null;
            for (int itr = 0; itr < tokenList.size(); ++itr) {
                final String tokenStr = tokenList.get(itr).toString();
                if (tokenList.get(itr).tokentype == ETokenType.ttidentifier || tokenStr.equals(".")) {
                    tableName += tokenList.get(itr).toString();
                    if (tokenToReplace == null) {
                        tokenToReplace = tokenList.get(itr);
                    }
                    else {
                        tokenList.get(itr).setString("");
                    }
                }
                else if (tokenToReplace != null) {
                    break;
                }
            }
            final DatabaseWriterConnection.TableNameParts tps = this.callbackIntf.getTableNameParts(tableName);
            final MappedName mappedName = this.getMappedName((tps.catalog == null) ? catalog : tps.catalog, (tps.schema == null) ? schema : tps.schema, tps.table);
            tokenToReplace.setString(mappedName.toString());
            ddlCmd = custStmt.toString();
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
