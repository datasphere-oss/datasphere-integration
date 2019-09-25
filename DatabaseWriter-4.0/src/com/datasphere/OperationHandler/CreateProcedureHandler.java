package com.datasphere.OperationHandler;

import gudusoft.gsqlparser.stmt.*;

import com.datasphere.intf.*;

import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.oracle.*;

public class CreateProcedureHandler extends StoredProcedureHandler
{
    public CreateProcedureHandler(final DBInterface intf) {
        super(intf);
    }
    
    @Override
    protected TObjectName getStoredProcedureName(final TCommonStoredProcedureSqlStatement stmt) {
        final TPlsqlCreateProcedure createProcedureStmt = (TPlsqlCreateProcedure)stmt;
        return createProcedureStmt.getProcedureName();
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
