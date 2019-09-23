package com.datasphere.runtime.compiler.stmts;

public class LimitClause
{
    public final int limit;
    public final int offset;
    
    public LimitClause(final int limit, final int offset) {
        this.limit = limit;
        this.offset = offset;
    }
}
