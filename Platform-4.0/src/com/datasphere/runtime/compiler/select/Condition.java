package com.datasphere.runtime.compiler.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.UnboxCastOperation;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.visitors.CheckNullRejection;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.NamePolicy;

public class Condition implements Comparable<Condition>
{
    public final List<CondExpr> exprs;
    public final BitSet dataSets;
    public final CondKey key;
    public final Map<Integer, List<Integer>> dsIndexes;
    public int idxCount;
    
    public Condition(final CondExpr firstExpr) {
        (this.exprs = new LinkedList<CondExpr>()).add(firstExpr);
        this.dataSets = firstExpr.datasets;
        this.key = firstExpr.getCondKey();
        this.dsIndexes = Factory.makeMap();
        this.idxCount = 0;
    }
    
    @Override
    public String toString() {
        return this.dataSets + " " + this.exprs;
    }
    
    @Override
    public int compareTo(final Condition o) {
        if (this == o) {
            return 0;
        }
        final int c1 = this.dataSets.cardinality();
        final int c2 = o.dataSets.cardinality();
        if (c1 != c2) {
            return (c1 < c2) ? -1 : 1;
        }
        if (this.idxCount > o.idxCount) {
            return -1;
        }
        if (this.idxCount < o.idxCount) {
            return 1;
        }
        return 0;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Condition)) {
            return false;
        }
        final Condition other = (Condition)o;
        return this.exprs.equals(other.exprs) && this.dataSets.equals(other.dataSets) && this.key.equals(other.key);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.key.hashCode()).append(this.dataSets.hashCode()).append(this.exprs.hashCode()).toHashCode();
    }
    
    public boolean isIndexable(final int relID) {
        assert this.dataSets.get(relID);
        return this.exprs.get(0).isIndexable(relID);
    }
    
    public boolean isSimilar(final CondExpr e) {
        return this.exprs.get(0).similar(e);
    }
    
    public List<Expr> getIndexExprs(final int relID) {
        final List<Expr> indexVector = new ArrayList<Expr>();
        for (final CondExpr e : this.exprs) {
            indexVector.add(e.getIndexExpr(relID));
        }
        return indexVector;
    }
    
    public List<Expr> getSearchExprs(final int relID) {
        final List<Expr> searchVector = new ArrayList<Expr>();
        for (final CondExpr e : this.exprs) {
            searchVector.add(e.getSearchExpr(relID));
        }
        return searchVector;
    }
    
    public boolean isJoinable() {
        return this.key.isJoinable();
    }
    
    public List<CondExpr> getCondExprs() {
        return this.exprs;
    }
    
    public Map<Integer, Set<String>> getCondFields() {
        final Map<Integer, Set<String>> ret = Factory.makeMap();
        for (final CondExpr e : this.exprs) {
            final List<Pair<Integer, String>> flds = e.getFields();
            for (final Pair<Integer, String> p : flds) {
                Set<String> set = ret.get(p.first);
                if (set == null) {
                    set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                    ret.put(p.first, set);
                }
                set.add(p.second);
            }
        }
        return ret;
    }
    
    public void updateFieldIndexesSupport(final int dsID, final List<Integer> lst) {
        assert this.dsIndexes.get(dsID) == null;
        this.dsIndexes.put(dsID, lst);
        this.idxCount += lst.size();
    }
    
    private static int findKeyFieldIndex(final List<String> indexFieldList, final String name) {
        int i = 0;
        for (final String fieldName : indexFieldList) {
            if (NamePolicy.isEqual(name, fieldName)) {
                return i;
            }
            ++i;
        }
        return -1;
    }
    
    public Pair<List<Expr>, List<Predicate>> getIndexSearchAndPostCondition(final int datasetID, final List<String> indexFieldList) {
        final List<Expr> idxExprs = this.getIndexExprs(datasetID);
        final List<Expr> searchExprs = this.getSearchExprs(datasetID);
        assert idxExprs.size() == searchExprs.size();
        final Expr[] searchExprsForIndex = new Expr[indexFieldList.size()];
        final List<Predicate> postFilters = new ArrayList<Predicate>();
        final Iterator<Expr> it = searchExprs.iterator();
        for (final Expr e : idxExprs) {
            final Expr se = it.next();
            int fieldIndex = -1;
            if (e instanceof FieldRef) {
                final FieldRef f = (FieldRef)e;
                final ValueExpr obj = f.getExpr();
                if (obj instanceof DataSetRef) {
                    final DataSetRef ref = (DataSetRef)obj;
                    assert datasetID == ref.getDataSet().getID();
                    fieldIndex = findKeyFieldIndex(indexFieldList, f.getName());
                }
            }
            if (fieldIndex != -1) {
                searchExprsForIndex[fieldIndex] = se;
            }
            else {
                final Predicate postFilter = new ComparePredicate(ExprCmd.EQ, Arrays.asList((ValueExpr)se, (ValueExpr)e));
                postFilters.add(postFilter);
            }
        }
        for (final Expr e2 : searchExprsForIndex) {
            assert e2 != null;
        }
        final List<Expr> searchExprsForIndexList = Arrays.asList(searchExprsForIndex);
        return Pair.make(searchExprsForIndexList, postFilters);
    }
    
    public List<Pair<FieldRef, ValueExpr>> getEqExprs() {
        final List<Pair<FieldRef, ValueExpr>> ret = new ArrayList<Pair<FieldRef, ValueExpr>>();
        for (final CondExpr e : this.exprs) {
            final Pair<FieldRef, ValueExpr> p = e.isEqPredicate();
            if (p != null) {
                ret.add(p);
            }
        }
        return ret;
    }
    
    public static class CondKey
    {
        public final BitSet leftset;
        public final BitSet rightset;
        
        public CondKey(final BitSet leftset, final BitSet rightset) {
            this.leftset = leftset;
            this.rightset = rightset;
        }
        
        @Override
        public int hashCode() {
            return 37 * this.leftset.hashCode() + this.rightset.hashCode();
        }
        
        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof CondKey)) {
                return false;
            }
            final CondKey o = (CondKey)other;
            return (this.leftset.equals(o.leftset) && this.rightset.equals(o.rightset)) || (this.leftset.equals(o.rightset) && this.rightset.equals(o.leftset));
        }
        
        public boolean isJoinable() {
            return !this.leftset.isEmpty() && !this.rightset.isEmpty();
        }
        
        @Override
        public String toString() {
            return "CondKey(" + this.leftset + "=" + this.rightset + ")";
        }
    }
    
    public static class CondExpr
    {
        public final Join.JoinNode joinInfo;
        public final Predicate expr;
        public final BitSet datasets;
        
        public CondExpr(final Predicate expr, final BitSet datasets, final Join.JoinNode joinInfo) {
            this.joinInfo = joinInfo;
            this.expr = expr;
            this.datasets = (BitSet)datasets.clone();
        }
        
        @Override
        public String toString() {
            return "(" + this.expr + ")" + ((this.joinInfo == null) ? "" : this.joinInfo);
        }
        
        public boolean similar(final CondExpr other) {
            return false;
        }
        
        public boolean isIndexable(final int relID) {
            return false;
        }
        
        public Expr getIndexExpr(final int relID) {
            return null;
        }
        
        public Expr getSearchExpr(final int relID) {
            return null;
        }
        
        public CondKey getCondKey() {
            return new CondKey(this.datasets, new BitSet());
        }
        
        public List<Pair<Integer, String>> getFields() {
            return null;
        }
        
        public boolean innerTableFilterInOuterJoin(final int ds) {
            if (this.joinInfo != null && this.joinInfo.kindOfJoin.isOuter()) {
                if (this.joinInfo.kindOfJoin.isLeft() && this.joinInfo.right.getDataSets().get(ds)) {
                    return true;
                }
                if (this.joinInfo.kindOfJoin.isRight() && this.joinInfo.left.getDataSets().get(ds)) {
                    return true;
                }
            }
            return false;
        }
        
        public boolean isOuterJoinCondtion(final BitSet alreadyJoined, final int datasetID) {
            if (this.joinInfo != null && this.joinInfo.kindOfJoin.isOuter()) {
                if (this.joinInfo.kindOfJoin.isLeft()) {
                    final BitSet outer = this.joinInfo.left.getDataSets();
                    final BitSet inner = this.joinInfo.right.getDataSets();
                    if (outer.intersects(alreadyJoined) && inner.get(datasetID)) {
                        return true;
                    }
                }
                if (this.joinInfo.kindOfJoin.isRight()) {
                    final BitSet outer = this.joinInfo.right.getDataSets();
                    final BitSet inner = this.joinInfo.left.getDataSets();
                    if (outer.intersects(alreadyJoined) && inner.get(datasetID)) {
                        return true;
                    }
                }
            }
            return false;
        }
        
        public boolean acceptNulls(final int datasetID) {
            return CheckNullRejection.acceptNulls(this.expr, datasetID);
        }
        
        public boolean rejectsNulls(final int datasetID) {
            return (this.joinInfo == null || this.joinInfo.kindOfJoin.isInner()) && this.datasets.get(datasetID) && !this.acceptNulls(datasetID);
        }
        
        private Pair<FieldRef, ValueExpr> isEqPred(ValueExpr l, final ValueExpr r) {
            if (r.isConst()) {
                if (l instanceof UnboxCastOperation) {
                    l = ((UnboxCastOperation)l).getExpr();
                }
                if (l instanceof FieldRef) {
                    return Pair.make(l, r);
                }
            }
            return null;
        }
        
        public Pair<FieldRef, ValueExpr> isEqPredicate() {
            if (this.expr.op == ExprCmd.EQ) {
                final ComparePredicate cp = (ComparePredicate)this.expr;
                if (cp.args.size() == 2) {
                    final ValueExpr e1 = cp.args.get(0);
                    final ValueExpr e2 = cp.args.get(1);
                    Pair<FieldRef, ValueExpr> ret = this.isEqPred(e1, e2);
                    if (ret == null) {
                        ret = this.isEqPred(e2, e1);
                    }
                    return ret;
                }
            }
            return null;
        }
    }
    
    public static class JoinExpr extends CondExpr
    {
        public final Expr left;
        public final BitSet leftset;
        public final Expr right;
        public final BitSet rightset;
        
        public JoinExpr(final Predicate expr, final BitSet datasets, final Expr left, final BitSet leftset, final Expr right, final BitSet rightset, final Join.JoinNode joinInfo) {
            super(expr, datasets, joinInfo);
            this.left = left;
            this.leftset = (BitSet)leftset.clone();
            this.right = right;
            this.rightset = (BitSet)rightset.clone();
        }
        
        @Override
        public String toString() {
            return "(" + this.expr + ") {" + this.leftset + "} (" + this.left + ") = (" + this.right + ") {" + this.rightset + "}";
        }
        
        @Override
        public boolean similar(final CondExpr other) {
            if (other instanceof JoinExpr) {
                final JoinExpr o = (JoinExpr)other;
                if (this.leftset.equals(o.leftset) && this.rightset.equals(o.rightset)) {
                    return true;
                }
                if (this.leftset.equals(o.rightset) && this.rightset.equals(o.leftset)) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public boolean isIndexable(final int relID) {
            return (this.leftset.get(relID) && this.leftset.cardinality() == 1) || (this.rightset.get(relID) && this.rightset.cardinality() == 1);
        }
        
        @Override
        public Expr getIndexExpr(final int relID) {
            assert this.isIndexable(relID);
            if (this.leftset.get(relID)) {
                assert this.leftset.cardinality() == 1;
                return this.left;
            }
            else {
                assert this.rightset.get(relID);
                assert this.rightset.cardinality() == 1;
                return this.right;
            }
        }
        
        @Override
        public Expr getSearchExpr(final int relID) {
            assert this.isIndexable(relID);
            if (this.leftset.get(relID)) {
                assert this.leftset.cardinality() == 1;
                return this.right;
            }
            else {
                assert this.rightset.get(relID);
                assert this.rightset.cardinality() == 1;
                return this.left;
            }
        }
        
        @Override
        public CondKey getCondKey() {
            return new CondKey(this.leftset, this.rightset);
        }
        
        @Override
        public List<Pair<Integer, String>> getFields() {
            final List<Pair<Integer, String>> ret = new ArrayList<Pair<Integer, String>>(2);
            if (this.left instanceof FieldRef) {
                final FieldRef f = (FieldRef)this.left;
                final ValueExpr obj = f.getExpr();
                if (obj instanceof DataSetRef) {
                    final DataSetRef ref = (DataSetRef)obj;
                    ret.add(Pair.make(ref.getDataSet().getID(), f.getName()));
                }
            }
            if (this.right instanceof FieldRef) {
                final FieldRef f = (FieldRef)this.right;
                final ValueExpr obj = f.getExpr();
                if (obj instanceof DataSetRef) {
                    final DataSetRef ref = (DataSetRef)obj;
                    ret.add(Pair.make(ref.getDataSet().getID(), f.getName()));
                }
            }
            return ret;
        }
    }
}
