package com.datasphere.runtime.compiler.select;

import java.util.List;
import java.util.Map;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.uuid.UUID;

public interface DataSource
{
    String toString();
    
    void resolveDataSource(final Resolver p0, final Map<String, DataSet> p1, final List<DataSourcePrimary> p2, final boolean p3) throws MetaDataRepositoryException;
    
    Join.Node addDataSource(final Resolver p0);
    
    public interface Resolver
    {
        Compiler getCompiler();
        
        int getNextDataSetID();
        
        Class<?> getTypeInfo(final TypeName p0);
        
        TraceOptions getTraceOptions();
        
        boolean isAdhoc();
        
        void addTypeID(final UUID p0);
        
        void addPredicate(final Predicate p0);
        
        Join.Node addDataSourceJoin(final DataSourceJoin p0);
        
        Join.Node addDataSet(final DataSet p0);
        
        void error(final String p0, final Object p1);
    }
}
