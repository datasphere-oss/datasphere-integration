package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ExportAppStmt extends Stmt
{
    public final String appName;
    public final String filepath;
    public final String format;
    
    public ExportAppStmt(final String appName, final String destJar, final String format) {
        this.appName = appName;
        this.filepath = destJar;
        this.format = format;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileExportTypesStmt(this);
    }
}
