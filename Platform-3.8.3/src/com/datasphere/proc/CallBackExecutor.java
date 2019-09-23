package com.datasphere.proc;

import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.uuid.UUID;

class CallBackExecutor implements Compiler.ExecutionCallback
{
    UUID uuid;
    
    @Override
    public void execute(final Stmt stmt, final Compiler compiler) throws Exception {
        this.uuid = (UUID)compiler.compileStmt(stmt);
    }
}
