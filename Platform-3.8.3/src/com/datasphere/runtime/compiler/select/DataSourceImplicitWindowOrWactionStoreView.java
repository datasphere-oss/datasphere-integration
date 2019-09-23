package com.datasphere.runtime.compiler.select;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.security.ObjectPermission;

public class DataSourceImplicitWindowOrHDStoreView extends DataSourcePrimary
{
    private final String streamOrHDStoreName;
    private final String alias;
    private final Pair<IntervalPolicy, IntervalPolicy> window_len;
    private final boolean isJumping;
    private final List<String> partitionFields;
    
    public DataSourceImplicitWindowOrHDStoreView(final String streamOrHDStoreName, final String alias, final Pair<IntervalPolicy, IntervalPolicy> window_len, final boolean isJumping, final List<String> part) {
        this.streamOrHDStoreName = streamOrHDStoreName;
        this.alias = alias;
        this.window_len = window_len;
        this.isJumping = isJumping;
        this.partitionFields = part;
    }
    
    @Override
    public String toString() {
        return this.streamOrHDStoreName + "[" + (this.isJumping ? "JUMPING " : "") + this.window_len + " " + this.partitionFields + "]" + this.alias;
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) throws MetaDataRepositoryException {
        final Compiler co = r.getCompiler();
        final Context ctx = co.getContext();
        final MetaInfo.MetaObject dataSource = ctx.getDataSourceInCurSchema(this.streamOrHDStoreName);
        if (dataSource == null) {
            r.error("no such stream", this.streamOrHDStoreName);
        }
        if (!PermissionUtility.checkPermission(dataSource, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
            r.error("No permission to select from " + dataSource.type.toString().toLowerCase() + " " + dataSource.nsName + "." + dataSource.name, dataSource.name);
        }
        if (dataSource instanceof MetaInfo.Stream) {
            final MetaInfo.Stream stream = (MetaInfo.Stream)dataSource;
            co.checkPartitionFields(this.partitionFields, stream.dataType, this.streamOrHDStoreName);
            final MetaInfo.Window w = ctx.putWindow(false, ctx.makeObjectName(RuntimeUtils.genRandomName("window")), stream.uuid, this.partitionFields, this.window_len, this.isJumping, false, true, null);
            w.getMetaInfoStatus().setAnonymous(true);
            MetadataRepository.getINSTANCE().updateMetaObject(w, ctx.getSessionID());
            r.addTypeID(stream.dataType);
            final Class<?> type = co.getTypeClass(stream.dataType);
            final DataSet ds = DataSetFactory.createWindowDS(r.getNextDataSetID(), this.streamOrHDStoreName, this.alias, type, w);
            return ds;
        }
        r.error("implicit window can be applied only to stream", this.streamOrHDStoreName);
        return null;
    }
}
