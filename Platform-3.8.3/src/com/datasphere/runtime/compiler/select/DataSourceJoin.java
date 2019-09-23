package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;
import com.datasphere.metaRepository.*;

public class DataSourceJoin implements DataSource
{
    public final DataSource left;
    public final DataSource right;
    public final Join.Kind kindOfJoin;
    public final Predicate joinCondition;
    
    public DataSourceJoin(final DataSource left, final DataSource right, final Join.Kind kindOfJoin, final Predicate join_cond) {
        this.left = left;
        this.right = right;
        this.kindOfJoin = kindOfJoin;
        this.joinCondition = join_cond;
    }
    
    @Override
    public String toString() {
        return this.left + " " + this.kindOfJoin + " JOIN " + this.right + " ON " + this.joinCondition;
    }
    
    @Override
    public void resolveDataSource(final Resolver r, final Map<String, DataSet> resolved, final List<DataSourcePrimary> unresolved, final boolean onlyPrimary) throws MetaDataRepositoryException {
        this.left.resolveDataSource(r, resolved, unresolved, onlyPrimary);
        this.right.resolveDataSource(r, resolved, unresolved, onlyPrimary);
    }
    
    @Override
    public Join.Node addDataSource(final Resolver r) {
        return r.addDataSourceJoin(this);
    }
}
