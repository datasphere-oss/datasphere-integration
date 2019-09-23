package com.datasphere.runtime.compiler.select;

import java.util.*;
import com.datasphere.metaRepository.*;

public abstract class DataSourcePrimary implements DataSource
{
    private DataSet dataset;
    
    @Override
    public void resolveDataSource(final Resolver r, final Map<String, DataSet> resolved, final List<DataSourcePrimary> unresolved, final boolean onlyPrimary) throws MetaDataRepositoryException {
        this.dataset = this.resolveDataSource(r, resolved, onlyPrimary);
        if (this.dataset != null) {
            final String dsname = this.dataset.getName();
            if (resolved.containsKey(dsname)) {
                r.error("data source name duplication", dsname);
            }
            resolved.put(dsname, this.dataset);
        }
        else {
            unresolved.add(this);
        }
    }
    
    public DataSet resolveDataSource(final Resolver r, final Map<String, DataSet> resolved, final boolean onlyPrimary) throws MetaDataRepositoryException {
        return this.resolveDataSourceImpl(r);
    }
    
    public abstract DataSet resolveDataSourceImpl(final Resolver p0) throws MetaDataRepositoryException;
    
    @Override
    public Join.Node addDataSource(final Resolver r) {
        assert this.dataset != null;
        return r.addDataSet(this.dataset);
    }
}
