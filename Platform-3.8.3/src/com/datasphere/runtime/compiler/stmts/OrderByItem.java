package com.datasphere.runtime.compiler.stmts;

import com.datasphere.runtime.compiler.exprs.*;

public class OrderByItem
{
    public final ValueExpr expr;
    public final boolean isAscending;
    
    public OrderByItem(final ValueExpr expr, final boolean isAscending) {
        this.expr = expr;
        this.isAscending = isAscending;
    }
}
