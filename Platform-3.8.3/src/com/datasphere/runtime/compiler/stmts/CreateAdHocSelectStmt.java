package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateAdHocSelectStmt extends CreateStmt
{
    public final Select select;
    public final String select_text;
    
    public CreateAdHocSelectStmt(final Select sel, final String select_text, final String queryName) {
        super(EntityType.CQ, queryName, false);
        this.select = sel;
        this.select_text = select_text;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + this.select;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateAdHocSelect(this);
    }
}
