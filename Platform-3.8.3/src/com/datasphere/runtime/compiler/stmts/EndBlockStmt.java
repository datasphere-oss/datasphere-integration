package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class EndBlockStmt extends Stmt
{
    public final String name;
    public final EntityType type;
    
    public EndBlockStmt(final String name, final EntityType type) {
        this.name = name;
        this.type = type;
    }
    
    @Override
    public String toString() {
        return "END " + this.type + " " + this.name;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileEndStmt(this);
    }
}
