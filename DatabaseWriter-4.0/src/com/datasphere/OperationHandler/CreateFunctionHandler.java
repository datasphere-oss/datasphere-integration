package com.datasphere.OperationHandler;

import gudusoft.gsqlparser.stmt.*;

import com.datasphere.intf.*;

import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.oracle.*;

public class CreateFunctionHandler extends StoredProcedureHandler
{
    public CreateFunctionHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    protected TObjectName getStoredProcedureName(final TCommonStoredProcedureSqlStatement stmt) {
        final TPlsqlCreateFunction createFunctionStmt = (TPlsqlCreateFunction)stmt;
        return createFunctionStmt.getFunctionName();
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
