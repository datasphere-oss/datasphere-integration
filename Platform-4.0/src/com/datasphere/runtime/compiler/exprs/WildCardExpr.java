package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public class WildCardExpr extends ValueExpr
{
    private final String dsname;
    private final List<ValueExpr> exprs;
    private final List<String> aliases;
    private boolean visited;
    
    public WildCardExpr(final String dsname) {
        super(ExprCmd.WILDCARD);
        this.visited = false;
        this.dsname = dsname;
        this.exprs = new ArrayList<ValueExpr>();
        this.aliases = new ArrayList<String>();
    }
    
    @Override
    public Class<?> getType() {
        return WildCardExprType.class;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitWildCardExpr(this, params);
    }
    
    public String getDataSetName() {
        return this.dsname;
    }
    
    public List<ValueExpr> getExprs() {
        return this.exprs;
    }
    
    public List<String> getAliases() {
        return this.aliases;
    }
    
    public void setExprs(final List<FieldRef> fields) {
        if (!this.visited) {
            this.exprs.addAll(fields);
            for (final FieldRef f : fields) {
                this.aliases.add(f.getName());
            }
            this.visited = true;
        }
    }
    
    public void updateExprs(final List<ValueExpr> exprs) {
        this.exprs.clear();
        this.exprs.addAll(exprs);
    }
    
    public static class WildCardExprType
    {
    }
}
