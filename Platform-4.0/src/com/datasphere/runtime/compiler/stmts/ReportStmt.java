package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class ReportStmt extends Stmt
{
    String cqname;
    int reportAction;
    List<String> params;
    public static final int START_REPORT = 1;
    public static final int END_REPORT = 2;
    public static final int LAG_REPORT = 3;
    
    public ReportStmt(final String cqname, final int reportAction, final List<String> param_list) {
        this.cqname = cqname;
        this.reportAction = reportAction;
        this.params = param_list;
    }
    
    public String getCqname() {
        return this.cqname;
    }
    
    public int getReportAction() {
        return this.reportAction;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        c.compileReportStmt(this.cqname, this.reportAction, this.params, c.getContext().getAuthToken());
        return this;
    }
}
