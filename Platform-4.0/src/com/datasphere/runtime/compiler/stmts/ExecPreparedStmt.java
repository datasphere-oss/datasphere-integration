package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;

public class ExecPreparedStmt extends Stmt
{
    public final String queryName;
    public final List<Property> params;
    
    public ExecPreparedStmt(final String queryName, final List<Property> params) {
        this.queryName = queryName;
        this.params = params;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.execPreparedQuery(this);
    }
}
