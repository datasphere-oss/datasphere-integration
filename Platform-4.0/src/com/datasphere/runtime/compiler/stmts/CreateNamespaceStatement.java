package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateNamespaceStatement extends CreateStmt
{
    public CreateNamespaceStatement(final String name, final Boolean doReplace) {
        super(EntityType.NAMESPACE, name, doReplace);
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateNamespaceStatement(this);
    }
}
