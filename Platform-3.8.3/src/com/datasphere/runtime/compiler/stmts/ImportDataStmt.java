package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ImportDataStmt extends Stmt
{
    public final String what;
    public final String where;
    public final Boolean replace;
    
    public ImportDataStmt(final String what, final String where, final Boolean replace) {
        this.what = what;
        this.where = where;
        this.replace = replace;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileImportDataStmt(this);
    }
}
