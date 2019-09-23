package com.datasphere.runtime.compiler.select;

import java.util.Collections;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.RuntimeUtils;

public class DataSourceView extends DataSourcePrimary
{
    private final Select subSelet;
    private final String alias;
    private final String selectText;
    
    public DataSourceView(final Select stmt, final String alias, final String selectText) {
        this.subSelet = stmt;
        this.alias = alias;
        this.selectText = selectText;
    }
    
    @Override
    public String toString() {
        return "(" + this.subSelet + ") AS " + this.alias;
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) throws MetaDataRepositoryException {
        final Compiler co = r.getCompiler();
        final Context ctx = co.getContext();
        final String cqName = RuntimeUtils.genRandomName("view");
        final Select select = this.subSelet;
        try {
            final CQExecutionPlan plan = SelectCompiler.compileSelect(cqName, ctx.getCurNamespaceName(), co, select, r.getTraceOptions());
            final MetaInfo.CQ cq = ctx.putCQ(false, ctx.makeObjectName(cqName), null, plan, this.selectText, Collections.emptyList(), null, null);
            cq.getMetaInfoStatus().setAnonymous(true);
            MetadataRepository.getINSTANCE().updateMetaObject(cq, ctx.getSessionID());
            final DataSet ds = DataSetFactory.createSelectViewDS(r.getNextDataSetID(), this.alias, cq.uuid, plan.resultSetDesc, plan.isStateful());
            return ds;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
