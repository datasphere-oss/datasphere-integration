package com.datasphere.runtime.compiler.select;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.security.ObjectPermission;

public class DataSourceHDStoreView extends DataSourcePrimary
{
    private final String hdStoreName;
    private final String alias;
    private final Interval window_len;
    private final Boolean isJumping;
    private final boolean subscribeToUpdates;
    
    public DataSourceHDStoreView(final String hdStoreName, final String alias, final Boolean isJumping, final Interval window_len, final boolean subscribeToUpdates) {
        this.hdStoreName = hdStoreName;
        this.alias = alias;
        this.window_len = window_len;
        this.subscribeToUpdates = subscribeToUpdates;
        this.isJumping = isJumping;
    }
    
    @Override
    public String toString() {
        return this.hdStoreName + "[" + this.window_len + (this.subscribeToUpdates ? " STREAM" : "") + "]" + this.alias;
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) throws MetaDataRepositoryException {
        final Compiler co = r.getCompiler();
        final Context ctx = co.getContext();
        final MetaInfo.MetaObject swc = ctx.getDataSourceInCurSchema(this.hdStoreName);
        if (!(swc instanceof MetaInfo.HDStore)) {
            r.error("no such hd store", this.hdStoreName);
        }
        if (!PermissionUtility.checkPermission(swc, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
            r.error("No permission to select from " + swc.type.toString().toLowerCase() + " " + swc.nsName + "." + swc.name, swc.name);
        }
        if (this.isJumping && this.subscribeToUpdates) {
            r.error("Jumping and push cannot be used together.", this.hdStoreName);
        }
        final MetaInfo.HDStore store = (MetaInfo.HDStore)swc;
        Class<?> type = null;
        try {
            final MetaInfo.Type typeMetaObject = (MetaInfo.Type)ctx.getObject(store.contextType);
            type = ClassLoader.getSystemClassLoader().loadClass(typeMetaObject.className);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        final MetaInfo.WAStoreView w = ctx.putWAStoreView(RuntimeUtils.genRandomName("wastoreview"), ctx.getCurNamespaceName(), store.uuid, (this.window_len == null) ? null : IntervalPolicy.createTimePolicy(this.window_len), this.isJumping, this.subscribeToUpdates, r.isAdhoc());
        final DataSet ds = DataSetFactory.createWAStoreDS(r.getNextDataSetID(), this.hdStoreName, this.alias, type, w, store);
        return ds;
    }
}
