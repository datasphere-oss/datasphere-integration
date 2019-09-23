package com.datasphere.runtime.compiler.stmts;

import java.io.Serializable;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class SetStmt extends Stmt
{
    public final String paramname;
    public final Serializable paramvalue;
    
    public SetStmt(final String paramname, final Serializable paramvalue) {
        this.paramname = paramname;
        this.paramvalue = paramvalue;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileSetStmt(this);
    }
}
