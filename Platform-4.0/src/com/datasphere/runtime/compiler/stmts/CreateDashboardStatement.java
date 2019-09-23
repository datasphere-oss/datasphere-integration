package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class CreateDashboardStatement extends Stmt
{
    private final String fileName;
    private final Boolean doReplace;
    
    public CreateDashboardStatement(final Boolean doReplace, final String fileName) {
        this.fileName = fileName;
        this.doReplace = doReplace;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateDashboardStatement(this);
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    public Boolean getDoReplace() {
        return this.doReplace;
    }
}
