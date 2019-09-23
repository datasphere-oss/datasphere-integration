package com.datasphere.runtime.compiler.stmts;

import java.io.*;
import com.datasphere.runtime.compiler.exprs.*;

public class SelectTarget implements Serializable
{
    public final ValueExpr expr;
    public final String alias;
    
    public SelectTarget(final ValueExpr expr, final String alias) {
        this.expr = expr;
        this.alias = alias;
    }
    
    @Override
    public String toString() {
        if (this.expr == null) {
            return "*";
        }
        return "" + this.expr + ((this.alias == null) ? "" : (" AS " + this.alias));
    }
}
