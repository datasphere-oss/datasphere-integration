package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ImportStmt extends Stmt
{
    public final boolean hasStar;
    public final boolean hasStaticKeyword;
    public final String name;
    
    public ImportStmt(final String name, final boolean hasStar, final boolean hasStaticKeyword) {
        this.name = name;
        this.hasStar = hasStar;
        this.hasStaticKeyword = hasStaticKeyword;
    }
    
    @Override
    public String toString() {
        return "IMPORT " + (this.hasStaticKeyword ? " STATIC " : "") + this.name + (this.hasStar ? ".*" : "");
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileImportStmt(this);
    }
}
