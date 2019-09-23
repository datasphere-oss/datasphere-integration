package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.components.EntityType;

public class CreateTypeStmt extends CreateStmt
{
    public final TypeDefOrName typeDef;
    
    public CreateTypeStmt(final String type_name, final Boolean doReplace, final TypeDefOrName typeDef) {
        super(EntityType.TYPE, type_name, doReplace);
        this.typeDef = typeDef;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " (" + this.typeDef + ")";
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateTypeStmt(this);
    }
}
