package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;

public class JoinDesc
{
    public final JoinAlgorithm algorithm;
    public final BitSet joinSet;
    public final DataSet withDataSet;
    public final List<Condition> afterJoinConditions;
    public final Condition joinCondition;
    public final IndexDesc joinIndex;
    public final List<Expr> searchExprs;
    public final List<Predicate> postFilters;
    public final int windowIndexID;
    public final boolean isOuter;
    
    public JoinDesc(final JoinAlgorithm algo, final BitSet joinSet, final DataSet dataset, final Condition jc, final IndexDesc id, final List<Expr> searchExprs, final List<Predicate> postFilters, final int windowIndexID, final boolean isOuter) {
        this.algorithm = algo;
        this.joinSet = (BitSet)joinSet.clone();
        this.withDataSet = dataset;
        this.afterJoinConditions = new ArrayList<Condition>(0);
        this.joinCondition = jc;
        this.joinIndex = id;
        this.searchExprs = searchExprs;
        this.postFilters = postFilters;
        this.windowIndexID = windowIndexID;
        this.isOuter = isOuter;
    }
    
    @Override
    public String toString() {
        return "JoinDesc(" + this.algorithm + " set " + this.joinSet + ((this.joinCondition == null) ? " crossjoin " : " join ") + this.withDataSet.getName() + " {" + this.withDataSet.getID() + "}" + ((this.joinCondition == null) ? "" : ("\n\ton " + this.joinCondition)) + ((this.joinIndex == null) ? "" : ("\n\tindex " + this.joinIndex)) + ((this.searchExprs == null) ? "" : ("\n\tsearch " + this.searchExprs)) + "\n\tpostFilters " + this.postFilters + "\n\twindowIndexID " + this.windowIndexID + "\n\tisOuter " + this.isOuter + "\n\tafter join filters " + this.afterJoinConditions + ")";
    }
    
    public Class<?>[] getSearchKeySignature() {
        return Expr.getSignature(this.searchExprs);
    }
    
    public enum JoinAlgorithm
    {
        SCAN, 
        INDEX_LOOKUP, 
        WINDOW_INDEX_LOOKUP, 
        WITH_STREAM;
    }
}
