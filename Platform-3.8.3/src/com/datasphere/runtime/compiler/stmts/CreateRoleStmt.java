package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateRoleStmt extends CreateStmt
{
    public CreateRoleStmt(final String name) {
        super(EntityType.ROLE, name, false);
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateRoleStmt(this);
    }
}
