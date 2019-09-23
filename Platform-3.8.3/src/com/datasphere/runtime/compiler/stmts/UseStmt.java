package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class UseStmt extends Stmt
{
    public final EntityType what;
    public final String schemaName;
    public final boolean recompile;
    
    public UseStmt(final EntityType what, final String schemaName, final boolean recompile) {
        this.what = what;
        this.schemaName = schemaName;
        this.recompile = recompile;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileUseStmt(this);
    }
    
    @Override
    public String toString() {
        if (this.sourceText != null && !this.sourceText.isEmpty()) {
            return this.sourceText;
        }
        return "USE " + this.schemaName;
    }
}
