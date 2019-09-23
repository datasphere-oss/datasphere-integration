package com.datasphere.runtime.compiler.stmts;

import java.util.ArrayList;
import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class MonitorStmt extends Stmt
{
    public final List<String> params;
    
    public MonitorStmt(final List<String> params) {
        if (params == null) {
            this.params = new ArrayList<String>();
        }
        else {
            this.params = params;
        }
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        c.compileMonitorStmt(this);
        return null;
    }
}
