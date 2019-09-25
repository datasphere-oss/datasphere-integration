package com.datasphere.OperationHandler;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.TSourceTokenList;

public class GrantHandler extends DDLHandler
{
    private Logger logger;
    
    public GrantHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)GrantHandler.class);
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
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        MappedName mappedName = null;
        String tokenToCompare = "TO";
        String ddlCmd = (String)event.data[0];
        this.sqlParser.sqltext = ddlCmd;
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        final int ret = this.sqlParser.parse();
        if (ret == 0) {
            final TCustomSqlStatement custStmt = this.sqlParser.sqlstatements.get(0);
            final TSourceTokenList tokens = custStmt.sourcetokenlist;
            if (tokens.get(0).toString().equalsIgnoreCase("REVOKE")) {
                tokenToCompare = "FROM";
            }
            if (tokens != null && tokens.size() > 3) {
                final String role = tokens.get(2).toString();
                if (role.equalsIgnoreCase("SELECT") || role.equalsIgnoreCase("DELETE") || role.equalsIgnoreCase("INSERT") || role.equalsIgnoreCase("EXECUTE") || role.equalsIgnoreCase("REFERENCES") || role.equalsIgnoreCase("USAGE")) {
                    int itr;
                    for (itr = 0; itr < tokens.size() && !tokens.get(itr).toString().equalsIgnoreCase(tokenToCompare); ++itr) {}
                    int xtr;
                    for (xtr = itr - 1; !tokens.get(xtr - 1).toString().trim().isEmpty(); --xtr) {
                        final TSourceToken srcTkn = tokens.get(xtr);
                    }
                    final String xtrStr = tokens.get(xtr).toString();
                    final StringBuilder objectName = new StringBuilder();
                    while (xtr < itr - 2) {
                        final TSourceToken srcTkn2 = tokens.get(xtr);
                        objectName.append(srcTkn2.toString());
                        srcTkn2.setString("");
                        ++xtr;
                    }
                    final TSourceToken tkn = tokens.get(itr - 2);
                    objectName.append(tkn.toString());
                    mappedName = this.getMappedName(catalog, schema, objectName.toString());
                    tkn.setString(mappedName.toString());
                }
            }
            ddlCmd = custStmt.toString();
            return ddlCmd;
        }
        final String errMsg = "Error while replacing table name {" + this.sqlParser.getErrormessage() + "}";
        throw new DatabaseWriterException(errMsg);
    }
    
    @Override
    public boolean objectBasedOperation() {
        return false;
    }
    
    @Override
    public String getComponentName(final HDEvent event) {
        return null;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
