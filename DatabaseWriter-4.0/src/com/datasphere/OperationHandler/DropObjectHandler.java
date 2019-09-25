package com.datasphere.OperationHandler;

import com.datasphere.exception.*;
import com.datasphere.intf.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import gudusoft.gsqlparser.*;
import java.sql.*;

public class DropObjectHandler extends DDLHandler
{
    public DropObjectHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    public void onDDLOperation(final String targetTable, final HDEvent event) throws DatabaseWriterException {
        this.callbackIntf.onDropTable(targetTable, event);
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
            final TCustomSqlStatement dropObjectStmt = this.sqlParser.sqlstatements.get(0);
            final TSourceTokenList tokenList = dropObjectStmt.sourcetokenlist;
            String objectName = "";
            String beforeText = "";
            String afterText = "";
            boolean isBefore = true;
            for (int tknItr = 0; tknItr < tokenList.size(); ++tknItr) {
                final TSourceToken token = tokenList.get(tknItr);
                final ETokenType tokenType = token.tokentype;
                if (tokenType == ETokenType.ttidentifier || token.toString().equals(".")) {
                    objectName += token.toString();
                    isBefore = false;
                }
                else if (isBefore) {
                    beforeText = beforeText + " " + token.toString();
                }
                else {
                    afterText = afterText + " " + token.toString();
                }
            }
            if (objectName.contains(".")) {
                mappedName = this.getMappedName(objectName);
            }
            else {
                mappedName = this.getMappedName(schema, objectName);
            }
            ddlCmd = beforeText + mappedName.toString() + afterText;
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
