package com.datasphere.OperationHandler;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.Mock.MockDBConnection;
import com.datasphere.Mock.MockDBWRiter;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.stmt.TCommonStoredProcedureSqlStatement;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateFunction;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreatePackage;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateProcedure;

public class CreatePackageHandler extends StoredProcedureHandler
{
    private Logger logger;
    
    public CreatePackageHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)CreatePackageHandler.class);
    }
    
    @Override
    protected TObjectName getStoredProcedureName(final TCommonStoredProcedureSqlStatement stmt) {
        final TPlsqlCreatePackage createPackageStmt = (TPlsqlCreatePackage)stmt;
        return createPackageStmt.getPackageName();
    }
    
    @Override
    protected void updateStatement(final String catalog, final String schema, final TCustomSqlStatement statement) {
        final MappedName mappedName = null;
        if (statement instanceof TCommonStoredProcedureSqlStatement) {
            TObjectName storedProcName = null;
            TStatementList stmtLst = null;
            if (statement instanceof TPlsqlCreateFunction) {
                storedProcName = ((TPlsqlCreateFunction)statement).getFunctionName();
                if (this.logger.isInfoEnabled()) {
                    this.logger.info("Parsing CreateFunction of {" + storedProcName.toString() + "}");
                }
                stmtLst = ((TPlsqlCreateFunction)statement).getBodyStatements();
            }
            else if (statement instanceof TPlsqlCreateProcedure) {
                storedProcName = ((TPlsqlCreateProcedure)statement).getProcedureName();
                if (this.logger.isInfoEnabled()) {
                    this.logger.info("Parsing CreateProcedure of {" + storedProcName.toString() + "}");
                }
                stmtLst = ((TPlsqlCreateProcedure)statement).getBodyStatements();
            }
            else if (statement instanceof TPlsqlCreatePackage) {
                storedProcName = ((TPlsqlCreatePackage)statement).getPackageName();
                if (this.logger.isInfoEnabled()) {
                    this.logger.info("Parsing CreatePackage of {" + storedProcName.toString() + "}");
                }
                stmtLst = ((TPlsqlCreatePackage)statement).getBodyStatements();
            }
            else {
                this.logger.warn("Unhandled class type {" + statement.getClass().toString() + "} in CreatePackageHandler");
            }
            for (int itr = 0; itr < stmtLst.size(); ++itr) {
                final TCustomSqlStatement stmt = stmtLst.get(itr);
                this.logger.debug("Going to parse {" + stmt.toString() + "}");
                if (stmt instanceof TPlsqlCreateFunction || stmt instanceof TPlsqlCreateProcedure || stmt instanceof TPlsqlCreatePackage) {
                    this.updateStatement(catalog, schema, stmt);
                }
                super.updateStatement(catalog, schema, stmtLst.get(itr));
            }
        }
        else {
            super.updateStatement(catalog, schema, statement);
        }
    }
    
    public static void main(final String[] args) throws Exception {
        final File sqlFile = new File("/Users/arul/package.sql");
        final byte[] sqlBytes = Files.readAllBytes(sqlFile.toPath());
        final String sql = new String(sqlBytes);
        final DatabaseWriterConnection connection = new MockDBConnection("jdbc:oracle:thin:@192.168.1.8:1521:orcl", null, null);
        final MockDBWRiter mockDBWR = new MockDBWRiter(connection, null, null);
        final HDEvent HDEvent = new HDEvent();
        HDEvent.data = new Object[1];
        (HDEvent.metadata = new HashMap()).put(Constant.SCHEMA_NAME, "");
        HDEvent.metadata.put(Constant.CATALOG_NAME, "");
        HDEvent.data[0] = sql;
        final CreatePackageHandler handler = new CreatePackageHandler(mockDBWR);
        final String modifledSQL = handler.generateDDL(HDEvent, null);
        System.out.println("SQL {" + modifledSQL + "}");
    }
    
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
