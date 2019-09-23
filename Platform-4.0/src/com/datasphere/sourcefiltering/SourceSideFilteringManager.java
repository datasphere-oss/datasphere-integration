package com.datasphere.sourcefiltering;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;

public class SourceSideFilteringManager implements Manager
{
    @Override
    public Object receive(final Handler handler, final Compiler compiler, final CreateSourceOrTargetStmt createSourceWithImplicitCQStmt) throws MetaDataRepositoryException {
        return handler.handle(compiler, createSourceWithImplicitCQStmt);
    }
}
