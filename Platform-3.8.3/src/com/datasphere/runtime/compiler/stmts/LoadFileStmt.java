package com.datasphere.runtime.compiler.stmts;

import com.datasphere.exception.FatalException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class LoadFileStmt extends Stmt
{
    String fileName;
    
    public LoadFileStmt(final String fname) {
        this.fileName = "";
        this.fileName = fname;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        try {
            return c.compileLoadFileStmt(this);
        }
        catch (Exception e) {
            throw new FatalException(e);
        }
    }
    
    public String getFileName() {
        return this.fileName;
    }
}
