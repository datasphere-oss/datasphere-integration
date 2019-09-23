package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class DumpStmt extends Stmt
{
    String cqname;
    int dumpmode;
    int limit;
    public static final int DUMP_INPUT = 1;
    public static final int DUMP_OUTPUT = 2;
    public static final int DUMP_INPUTOUTPUT = 3;
    public static final int DUMP_CODE = 4;
    public static final int DEFAULT_LIMIT = 1000;
    
    public DumpStmt(final String cqname, final int mode, int limitVal) {
        this.cqname = cqname;
        this.dumpmode = mode;
        if (limitVal == Integer.MAX_VALUE) {
            limitVal = 1000;
        }
        this.limit = limitVal;
    }
    
    public String getCqname() {
        return this.cqname;
    }
    
    public int getDumpmode() {
        return this.dumpmode;
    }
    
    public int getLimit() {
        return this.limit;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        c.compileDumpStmt(this.cqname, this.dumpmode, this.limit, c.getContext().getAuthToken());
        return this;
    }
}
