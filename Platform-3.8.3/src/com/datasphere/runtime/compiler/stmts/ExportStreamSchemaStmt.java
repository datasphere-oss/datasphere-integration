package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ExportStreamSchemaStmt extends Stmt
{
    public final String streamName;
    public final String optPath;
    public final String optFileName;
    
    public ExportStreamSchemaStmt(final String streamname, final String optPath, final String optFileName) {
        this.streamName = streamname;
        this.optPath = optPath;
        this.optFileName = optFileName;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.exportStreamSchema(this);
    }
}
