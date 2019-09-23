package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class EmptyStmt extends Stmt
{
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return null;
    }
}
