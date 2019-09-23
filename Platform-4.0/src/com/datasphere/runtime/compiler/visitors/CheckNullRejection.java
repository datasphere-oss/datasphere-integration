package com.datasphere.runtime.compiler.visitors;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.InstanceOfPredicate;
import com.datasphere.runtime.compiler.exprs.LogicalPredicate;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.select.DataSet;
import com.datasphere.runtime.compiler.visitors.CheckNullRejection.Param;

public class CheckNullRejection extends ExpressionVisitorDefaultImpl<Param>
{
    private List<Param> args;
    private Param ret;
    private final int innerDS;
    
    public static boolean acceptNulls(final Predicate p, final int innerDS) {
        final CheckNullRejection c = new CheckNullRejection(p, innerDS);
        return c.acceptNulls();
    }
    
    private CheckNullRejection(final Predicate p, final int innerDS) {
        this.args = new ArrayList<Param>();
        this.ret = new Param();
        this.innerDS = innerDS;
        this.visitExpr(p, this.ret);
    }
    
    private boolean acceptNulls() {
        return this.ret.acceptNull;
    }
    
    @Override
    public Expr visitExpr(final Expr e, final Param param) {
        final List<Param> xargs = new ArrayList<Param>();
        for (final Expr arg : e.getArgs()) {
            final Param p = new Param();
            this.visitExpr(arg, p);
            xargs.add(p);
            param.dataSets.or(p.dataSets);
        }
        this.args = xargs;
        e.visit(this, param);
        return null;
    }
    
    @Override
    public Expr visitDataSetRef(final DataSetRef ref, final Param param) {
        final DataSet ds = ref.getDataSet();
        param.dataSets.set(ds.getID());
        return null;
    }
    
    @Override
    public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final Param param) {
        boolean acceptNull = false;
        switch (logicalPredicate.op) {
            case AND: {
                acceptNull = true;
                for (final Param p : this.args) {
                    if (!p.acceptNull) {
                        acceptNull = false;
                        break;
                    }
                }
                break;
            }
            case OR: {
                acceptNull = false;
                for (final Param p : this.args) {
                    if (p.acceptNull) {
                        acceptNull = true;
                        break;
                    }
                }
                break;
            }
            case NOT: {
                acceptNull = !this.args.get(0).acceptNull;
                break;
            }
            default: {
                assert false;
                break;
            }
        }
        param.acceptNull = acceptNull;
        return null;
    }
    
    @Override
    public Expr visitComparePredicate(final ComparePredicate comparePredicate, final Param param) {
        if (comparePredicate.op == ExprCmd.ISNULL && param.dataSets.get(this.innerDS)) {
            param.acceptNull = true;
        }
        else {
            param.acceptNull = !param.dataSets.get(this.innerDS);
        }
        return null;
    }
    
    @Override
    public Expr visitInstanceOfPredicate(final InstanceOfPredicate ins, final Param param) {
        param.acceptNull = !param.dataSets.get(this.innerDS);
        return null;
    }
    
    public static class Param
    {
        BitSet dataSets;
        boolean acceptNull;
        
        public Param() {
            this.dataSets = new BitSet();
        }
    }
}
