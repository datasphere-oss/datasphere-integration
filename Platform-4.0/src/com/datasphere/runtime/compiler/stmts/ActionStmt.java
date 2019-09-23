package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class ActionStmt extends Stmt
{
    public final ActionType what;
    public final String name;
    public final EntityType type;
    public final RecoveryDescription recov;
    
    public ActionStmt(final ActionType what, final String name, final EntityType type) {
        this.what = what;
        this.name = name;
        this.type = type;
        this.recov = null;
    }
    
    public ActionStmt(final ActionType what, final String name, final EntityType type, final RecoveryDescription recov) {
        this.what = what;
        this.name = name;
        this.type = type;
        this.recov = recov;
    }
    
    @Override
    public String toString() {
        return this.what + " " + this.type + " " + this.name;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileActionStmt(this);
    }
}
