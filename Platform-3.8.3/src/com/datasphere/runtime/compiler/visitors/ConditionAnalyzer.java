package com.datasphere.runtime.compiler.visitors;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.LogicalPredicate;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.select.Condition;
import com.datasphere.runtime.compiler.select.DataSet;
import com.datasphere.runtime.compiler.select.Join;

public class ConditionAnalyzer extends ExpressionVisitorDefaultImpl<BitSet>
{
    private final Predicate predicate;
    private int depth;
    private List<BitSet> argSets;
    private Condition.JoinExpr equijoin;
    private final BitSet datasets;
    private final Join.JoinNode joinInfo;
    
    public ConditionAnalyzer(final Predicate p, final Join.JoinNode jn) {
        this.joinInfo = jn;
        this.predicate = p;
        this.depth = 0;
        this.argSets = new ArrayList<BitSet>();
        this.datasets = new BitSet();
        this.equijoin = null;
        this.visitExpr(p, this.datasets);
    }
    
    public Condition.CondExpr getCondExpr() {
        if (this.equijoin != null) {
            return this.equijoin;
        }
        return new Condition.CondExpr(this.predicate, this.datasets, this.joinInfo);
    }
    
    @Override
    public Expr visitExpr(final Expr e, final BitSet bs) {
        ++this.depth;
        final List<BitSet> argSets = new ArrayList<BitSet>();
        for (final Expr arg : e.getArgs()) {
            final BitSet argbs = new BitSet();
            this.visitExpr(arg, argbs);
            argSets.add(argbs);
            bs.or(argbs);
        }
        this.argSets = argSets;
        e.visit(this, bs);
        --this.depth;
        return null;
    }
    
    @Override
    public Expr visitDataSetRef(final DataSetRef ref, final BitSet bs) {
        final DataSet ds = ref.getDataSet();
        bs.set(ds.getID());
        return null;
    }
    
    @Override
    public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final BitSet bs) {
        if (this.depth == 1 && logicalPredicate.op == ExprCmd.AND) {
            throw new AssertionError((Object)"AND predicate in condition");
        }
        return null;
    }
    
    @Override
    public Expr visitComparePredicate(final ComparePredicate comparePredicate, final BitSet bs) {
        if (this.depth == 1 && comparePredicate.op == ExprCmd.EQ && bs.cardinality() >= 2) {
            final BitSet leftbs = this.argSets.get(0);
            final BitSet rightbs = this.argSets.get(1);
            if (leftbs.cardinality() == 1 || rightbs.cardinality() == 1) {
                final ValueExpr left = comparePredicate.args.get(0);
                final ValueExpr right = comparePredicate.args.get(1);
                this.equijoin = new Condition.JoinExpr(this.predicate, bs, left, leftbs, right, rightbs, this.joinInfo);
            }
        }
        return null;
    }
}
