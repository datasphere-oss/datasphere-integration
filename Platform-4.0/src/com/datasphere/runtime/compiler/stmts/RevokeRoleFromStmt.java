package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class RevokeRoleFromStmt extends Stmt
{
    public final EntityType towhat;
    public final List<String> rolename;
    public final String name;
    
    public RevokeRoleFromStmt(final List<String> rolename, final String name, final EntityType what) {
        this.towhat = what;
        this.rolename = rolename;
        this.name = name;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.RevokeRoleFromStmt(this);
    }
}
