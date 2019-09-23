package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateSorterStmt extends CreateStmt
{
    public final Interval sortTimeInterval;
    public final List<SorterInOutRule> inOutRules;
    public final String errorStream;
    
    public CreateSorterStmt(final String name, final Boolean doReplace, final Interval sortInterval, final List<SorterInOutRule> inOutRules, final String errorStream) {
        super(EntityType.SORTER, name, doReplace);
        this.sortTimeInterval = sortInterval;
        this.inOutRules = inOutRules;
        this.errorStream = errorStream;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " OVER " + this.inOutRules + " WITHIN " + this.sortTimeInterval + " OUPUT ERRORS TO " + this.errorStream;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateSorterStmt(this);
    }
}
