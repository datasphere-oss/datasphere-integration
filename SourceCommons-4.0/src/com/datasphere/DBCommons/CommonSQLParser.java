package com.datasphere.DBCommons;

import org.apache.log4j.*;
import org.json.*;
import java.util.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;

public class CommonSQLParser
{
    protected TGSqlParser sqlparser;
    Logger logger;
    protected JSONObject ddlMeta;
    
    public CommonSQLParser() {
        this.logger = Logger.getLogger((Class)CommonSQLParser.class);
        this.ddlMeta = null;
        this.sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
    }
    
    public CommonSQLParser(final String dbName) {
        this.logger = Logger.getLogger((Class)CommonSQLParser.class);
        this.ddlMeta = null;
        switch (dbName) {
            case "Oracle": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
                break;
            }
            case "MySQL": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvmysql);
                break;
            }
            case "MSSQL": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
                break;
            }
            case "GreenPlum": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvgreenplum);
                break;
            }
            case "Sybase": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvsybase);
                break;
            }
            case "DB2-LUW": {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvdb2);
                break;
            }
            default: {
                this.sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
                break;
            }
        }
        this.ddlMeta = new JSONObject();
    }
    
    public void clealJSONMeta() {
        this.ddlMeta.remove("OperationName");
        this.ddlMeta.remove("OperationSubName");
        this.ddlMeta.remove("CatalogObjectType");
        this.ddlMeta.remove("CatalogObjectName");
        this.ddlMeta.remove("SQLStmt");
        this.ddlMeta.remove("ObjectName");
        this.ddlMeta.remove("ConstraintName");
        this.ddlMeta.remove("ConstraintType");
        this.ddlMeta.remove("TableMeta");
    }
    
    public HashMap<String, String> parseSelectStatement(final String sqlStmt) {
        this.sqlparser.sqltext = sqlStmt;
        HashMap<String, String> whereClauseColumns = null;
        if (this.sqlparser.parse() == 0) {
            final TCustomSqlStatement stmt = this.sqlparser.sqlstatements.get(0);
            if (stmt.sqlstatementtype == ESqlStatementType.sstselect) {
                final TSelectSqlStatement sStmt = (TSelectSqlStatement)this.sqlparser.sqlstatements.get(0);
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)"Parsing the where clause: ");
                }
                if (!sStmt.isCombinedQuery()) {
                    for (int i = 0; i < sStmt.getResultColumnList().size(); ++i) {
                        final TResultColumn resultColumn = stmt.getResultColumnList().getResultColumn(i);
                        if (this.logger.isInfoEnabled()) {
                            this.logger.info((Object)("Column: " + resultColumn.getExpr().toString()));
                        }
                    }
                    final WhereCondition whereClause = new WhereCondition(sStmt.getWhereClause().getCondition());
                    if (this.logger.isInfoEnabled()) {
                        this.logger.info((Object)("Where Clause: " + sStmt.getWhereClause().toString()));
                    }
                    whereClauseColumns = whereClause.parseColumns();
                }
            }
        }
        return whereClauseColumns;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
