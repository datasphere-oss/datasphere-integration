package com.datasphere.runtime.compiler.stmts;

import com.datasphere.drop.DropMetaObject;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class DropStmt extends Stmt
{
    public final EntityType objectType;
    public final String objectName;
    public final DropMetaObject.DropRule dropRule;
    
    public DropStmt(final EntityType objectType, final String objectName, final DropMetaObject.DropRule dropRule) {
        this.objectType = objectType;
        this.objectName = objectName;
        this.dropRule = dropRule;
    }
    
    @Override
    public String toString() {
        return "DROP " + this.objectType + " " + this.objectName;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileDropStmt(this);
    }
}
