package com.datasphere.runtime.compiler.select;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.proc.events.DynamicEvent;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.security.ObjectPermission;
import com.datasphere.uuid.UUID;

public class DataSourceStream extends DataSourcePrimary implements Serializable
{
    private final String streamName;
    private final String alias;
    private final String opt_typename;
    
    public DataSourceStream(final String stream_name, final String opt_typename, final String alias) {
        this.streamName = stream_name;
        this.opt_typename = opt_typename;
        this.alias = alias;
    }
    
    @Override
    public String toString() {
        return this.streamName + ((this.opt_typename == null) ? "" : ("<" + this.opt_typename + "> ")) + ((this.alias == null) ? "" : (" AS " + this.alias));
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) throws MetaDataRepositoryException {
        final Compiler co = r.getCompiler();
        final Context ctx = co.getContext();
        final MetaInfo.MetaObject swc = ctx.getDataSourceInCurSchema(this.streamName);
        if (swc == null) {
            r.error("no such stream", this.streamName);
        }
        if (!PermissionUtility.checkPermission(swc, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
            r.error("No permission to select from " + swc.type.toString().toLowerCase() + " " + swc.nsName + "." + swc.name, swc.name);
        }
        UUID dataTypeID = null;
        DataSet ds;
        if (swc instanceof MetaInfo.Stream && this.opt_typename != null) {
            final MetaInfo.Stream stream = (MetaInfo.Stream)swc;
            dataTypeID = stream.dataType;
            final Class<?> type = co.getTypeClass(dataTypeID);
            if (!DynamicEvent.class.isAssignableFrom(type)) {
                r.error("type of events in data source should be derived from " + DynamicEvent.class.getName(), this.streamName);
            }
            final MetaInfo.Type t = ctx.getTypeInCurSchema(this.opt_typename);
            if (t == null) {
                r.error("no such type", this.opt_typename);
            }
            final List<RSFieldDesc> fields = new ArrayList<RSFieldDesc>();
            for (final Map.Entry<String, String> fieldDesc : t.fields.entrySet()) {
                try {
                    final String fieldName = fieldDesc.getKey();
                    final Class<?> fieldType = ctx.getClass(fieldDesc.getValue());
                    final RSFieldDesc f = new RSFieldDesc(fieldName, fieldType);
                    fields.add(f);
                }
                catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            ds = DataSetFactory.createTranslatedDS(r.getNextDataSetID(), this.streamName, this.alias, fields, t.uuid, swc);
            final List<ValueExpr> args = new ArrayList<ValueExpr>();
            args.add(DataSetRef.makeDataSetRef(ds));
            r.addPredicate(new ComparePredicate(ExprCmd.CHECKRECTYPE, args));
        }
        else if (swc instanceof MetaInfo.HDStore) {
            final MetaInfo.HDStore store = (MetaInfo.HDStore)swc;
            dataTypeID = store.contextType;
            final Class<?> type = co.getTypeClass(dataTypeID);
            final MetaInfo.WAStoreView w = ctx.putWAStoreView(RuntimeUtils.genRandomName("wastoreview"), ctx.getCurNamespaceName(), store.uuid, null, null, false, r.isAdhoc());
            ds = DataSetFactory.createWAStoreDS(r.getNextDataSetID(), this.streamName, this.alias, type, w, store);
        }
        else if (swc instanceof MetaInfo.Window) {
            final MetaInfo.Window window = (MetaInfo.Window)swc;
            final MetaInfo.Stream stream2 = (MetaInfo.Stream)ctx.getObject(window.stream);
            final Class<?> type2 = co.getTypeClass(stream2.dataType);
            ds = DataSetFactory.createWindowDS(r.getNextDataSetID(), this.streamName, this.alias, type2, window);
        }
        else if (swc instanceof MetaInfo.Cache) {
            final MetaInfo.Cache cache = (MetaInfo.Cache)swc;
            final Class<?> type = co.getTypeClass(cache.typename);
            ds = DataSetFactory.createCacheDS(r.getNextDataSetID(), this.streamName, this.alias, type, cache);
        }
        else if (swc instanceof MetaInfo.Stream) {
            final MetaInfo.Stream stream = (MetaInfo.Stream)swc;
            final Class<?> type = co.getTypeClass(stream.dataType);
            ds = DataSetFactory.createStreamDS(r.getNextDataSetID(), this.streamName, this.alias, type, stream);
        }
        else {
            r.error(swc.type + " data source is not implemented yet", this.streamName);
            ds = null;
        }
        r.addTypeID(dataTypeID);
        return ds;
    }
}
