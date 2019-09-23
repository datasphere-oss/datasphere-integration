package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ExportDataStmt extends Stmt
{
    public final String what;
    public final String where;
    
    public ExportDataStmt(final String what, final String where) {
        this.what = what;
        this.where = where;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileExportDataStmt(this);
    }
}
