package com.datasphere.runtime.compiler.select;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.ObjFieldRef;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.exprs.VirtFieldRef;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.uuid.UUID;

public class DataSetBuilder
{
    private static final int ROW_AS_FIELD = -2;
    private final int id;
    private final DataSet.Kind kind;
    private final Class<?> dsType;
    private final String dsName;
    private String fullName;
    private Boolean isStateful;
    private UUID uuid;
    private boolean nested;
    private FieldAccessor fieldAccessor;
    private RowAccessor rowAccessor;
    private IndexInfo indexInfo;
    private IteratorInfo iterInfo;
    private JsonWAStoreInfo jsonWAStoreInfo;
    private TraslatedSchemaInfo transSchemaInfo;
    private FieldFactory fieldFactory;
    private List<Pair<String, Class<?>>> virtFields;
    
    static List<Field> getFields(final Class<?> type) {
        final List<Field> nsfields = CompilerUtils.getNonStaticFields(type);
        final List<Field> flds = CompilerUtils.getNotSpecial(nsfields);
        return flds;
    }
    
    static List<RSFieldDesc> makeRSDesc(final Class<?> type) {
        final List<Field> flds = getFields(type);
        final List<RSFieldDesc> ret = new ArrayList<RSFieldDesc>();
        for (final Field f : flds) {
            ret.add(new RSFieldDesc(f.getName(), f.getType()));
        }
        return ret;
    }
    
    public static boolean isRowAsField(final VirtFieldRef fld) {
        return fld.getIndex() == -2;
    }
    
    public DataSetBuilder(final int id, final DataSet.Kind kind, final String dsName, final Class<?> dsType) {
        this.id = id;
        this.kind = kind;
        this.dsName = dsName;
        this.dsType = dsType;
    }
    
    DataSetBuilder addRSDescFields(final List<RSFieldDesc> fields) {
        this.fieldFactory = new FieldFactory() {
            @Override
            public List<Pair<String, Class<?>>> getAllTypeFieldsImpl() {
                final List<Pair<String, Class<?>>> ret = new ArrayList<Pair<String, Class<?>>>();
                for (final RSFieldDesc field : fields) {
                    ret.add(Pair.make(field.name, field.type));
                }
                return ret;
            }
            
            @Override
            public List<FieldRef> makeListOfAllFields(final ValueExpr obj, final FieldAccessor fa) {
                final List<FieldRef> ret = new ArrayList<FieldRef>();
                int i = 0;
                for (final RSFieldDesc field : fields) {
                    final VirtFieldRef fld = new VirtFieldRef(obj, field.name, field.type, i, fa);
                    ret.add(fld);
                    ++i;
                }
                return ret;
            }
            
            @Override
            public FieldRef makeFieldRefImpl(final ValueExpr obj, final FieldAccessor fa, final String fieldName) throws NoSuchFieldException, SecurityException {
                int i = 0;
                for (final RSFieldDesc field : fields) {
                    if (NamePolicy.isEqual(field.name, fieldName)) {
                        final VirtFieldRef fld = new VirtFieldRef(obj, field.name, field.type, i, fa);
                        return fld;
                    }
                    ++i;
                }
                throw new NoSuchFieldException(fieldName);
            }
            
            @Override
            public String toString() {
                return "ResultSet" + fields + " field factory";
            }
        };
        return this;
    }
    
    DataSetBuilder addUnstructredFields(final Class<?> dsType, final Class<?> dynFldType) {
        this.fieldFactory = new FieldFactory() {
            @Override
            public List<Pair<String, Class<?>>> getAllTypeFieldsImpl() {
                return Collections.emptyList();
            }
            
            @Override
            public List<FieldRef> makeListOfAllFields(final ValueExpr obj, final FieldAccessor fa) {
                final FieldRef ref = new VirtFieldRef(obj, DataSetBuilder.this.dsName, dsType, -2, fa);
                return Collections.singletonList(ref);
            }
            
            @Override
            public FieldRef makeFieldRefImpl(final ValueExpr obj, final FieldAccessor fa, final String fieldName) throws NoSuchFieldException, SecurityException {
                return new VirtFieldRef(obj, fieldName, dynFldType, 0, fa);
            }
            
            @Override
            public String toString() {
                return "dynamic " + dsType.getCanonicalName() + " field factory";
            }
        };
        return this;
    }
    
    DataSetBuilder addJavatypeFields(final Class<?> dsType) {
        this.fieldFactory = new FieldFactory() {
            @Override
            public List<Pair<String, Class<?>>> getAllTypeFieldsImpl() {
                final List<Pair<String, Class<?>>> ret = new ArrayList<Pair<String, Class<?>>>();
                for (final Field field : dsType.getFields()) {
                    ret.add(Pair.make(field.getName(), field.getType()));
                }
                return ret;
            }
            
            @Override
            public List<FieldRef> makeListOfAllFields(final ValueExpr obj, final FieldAccessor fa) {
                final List<FieldRef> fields = new ArrayList<FieldRef>();
                for (final Field f : DataSetBuilder.getFields(dsType)) {
                    final FieldRef r = new ObjFieldRef(obj, f.getName(), f);
                    fields.add(r);
                }
                return fields;
            }
            
            @Override
            public FieldRef makeFieldRefImpl(final ValueExpr obj, final FieldAccessor fa, final String fieldName) throws NoSuchFieldException, SecurityException {
                final Field field = NamePolicy.getField(dsType, fieldName);
                return new ObjFieldRef(obj, fieldName, field);
            }
            
            @Override
            public String toString() {
                return "java type " + dsType.getCanonicalName() + " field factory";
            }
        };
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public String toString() {
                return "java object field accessor";
            }
        };
        return this;
    }
    
    DataSetBuilder addWindowIndexInfo(final MetaInfo.Window wi) {
        this.indexInfo = new IndexInfo() {
            @Override
            public List<Integer> getListOfIndexesForFields(final Set<String> set) {
                if (!wi.partitioningFields.isEmpty()) {
                    boolean windowPartitionKeyInCond = true;
                    for (final String fieldName : wi.partitioningFields) {
                        if (!set.contains(fieldName)) {
                            windowPartitionKeyInCond = false;
                            break;
                        }
                    }
                    if (windowPartitionKeyInCond) {
                        return Collections.singletonList(0);
                    }
                }
                return Collections.emptyList();
            }
            
            @Override
            public List<String> indexFieldList(final int idxID) {
                if (idxID != 0) {
                    return Collections.emptyList();
                }
                assert !wi.partitioningFields.isEmpty();
                return wi.partitioningFields;
            }
            
            @Override
            public String toString() {
                return "window " + wi.name + " index info";
            }
        };
        return this;
    }
    
    DataSetBuilder addCacheIndexInfo(final MetaInfo.Cache ci) {
        this.indexInfo = new IndexInfo() {
            @Override
            public List<Integer> getListOfIndexesForFields(final Set<String> set) {
                final Object o = ci.query_properties.get("keytomap");
                if (o instanceof String) {
                    final String keyfield = (String)o;
                    if (set.contains(keyfield)) {
                        return Collections.singletonList(0);
                    }
                }
                return Collections.emptyList();
            }
            
            @Override
            public List<String> indexFieldList(final int idxID) {
                if (idxID == 0) {
                    final Object o = ci.query_properties.get("keytomap");
                    if (o instanceof String) {
                        final String keyfield = (String)o;
                        return Collections.singletonList(keyfield);
                    }
                }
                return Collections.emptyList();
            }
            
            @Override
            public String toString() {
                return "cache " + ci.name + " index info";
            }
        };
        return this;
    }
    
    DataSetBuilder addSchemaTranslation(final UUID expectedTypeID) {
        this.transSchemaInfo = new TraslatedSchemaInfo() {
            @Override
            public UUID expectedTypeID() {
                return expectedTypeID;
            }
            
            @Override
            public String toString() {
                return "expected type uuid " + expectedTypeID;
            }
        };
        this.rowAccessor = new RowAccessor() {
            @Override
            public String genRowAccessor(final String obj) {
                return "getRowDataAndTranslate(" + obj + ", " + DataSetBuilder.this.id + ")";
            }
            
            @Override
            public String toString() {
                return "getRowDataAndTranslate(" + DataSetBuilder.this.id + ") row accessor";
            }
        };
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                return "getDynamicField(" + obj + "," + f.getIndex() + "," + DataSetBuilder.this.id + ")";
            }
            
            @Override
            public String toString() {
                return "getDynamicField(" + DataSetBuilder.this.id + ") field accessor";
            }
        };
        return this;
    }
    
    DataSetBuilder addIteratorInfo(final DataSet parent, final FieldRef parentField) {
        this.fullName = parent.getName() + "." + parentField.getName();
        this.isStateful = true;
        this.uuid = null;
        this.nested = true;
        this.iterInfo = new IteratorInfo() {
            @Override
            public DataSet getParent() {
                return parent;
            }
            
            @Override
            public FieldRef getIterableField() {
                return parentField;
            }
            
            @Override
            public String toString() {
                return "Iterator(" + parent.getFullName() + ", " + parentField.getName() + ")";
            }
        };
        return this;
    }
    
    DataSetBuilder addJsonWAStoreInfo(final MetaInfo.WAStoreView view, final MetaInfo.HDStore store) {
        this.jsonWAStoreInfo = new JsonWAStoreInfo() {
            @Override
            public MetaInfo.WAStoreView getStoreView() {
                return view;
            }
            
            @Override
            public MetaInfo.HDStore getStore() {
                return store;
            }
            
            @Override
            public String toString() {
                return "json wastore " + store.name + " info";
            }
        };
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                return "dynamicCast(" + obj + ".getData(), \"" + f.getName() + "\", " + f.getTypeName() + ".class)";
            }
            
            @Override
            public String toString() {
                return "JsonNodeEvent.getData() json dynamicCast(" + DataSetBuilder.this.id + ") field accessor";
            }
        };
        this.addVirtualField("$timestamp", DateTime.class);
        this.addVirtualField("$id", UUID.class);
        return this;
    }
    
    DataSetBuilder addVirtualField(final String name, final Class<?> type) {
        if (this.virtFields == null) {
            this.virtFields = new ArrayList<Pair<String, Class<?>>>();
        }
        this.virtFields.add(Pair.make(name, type));
        return this;
    }
    
    DataSetBuilder addStateful_UUID_FullName(final boolean isStateful, final UUID uuid, final String fullName) {
        this.isStateful = isStateful;
        this.uuid = uuid;
        this.fullName = fullName;
        return this;
    }
    
    DataSetBuilder addPayloadFieldAccessor() {
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                return obj + ".payload[" + f.getIndex() + "]";
            }
            
            @Override
            public String toString() {
                return "payload[] field accessor";
            }
        };
        return this;
    }
    
    DataSetBuilder addJavaReflectFieldAccessor() {
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                if (DataSetBuilder.isRowAsField(f)) {
                    return obj;
                }
                return "getJavaObjectField(" + obj + ", \"" + f.getName() + "\")";
            }
            
            @Override
            public String toString() {
                return "java getJavaObjectField() field accessor";
            }
        };
        return this;
    }
    
    DataSetBuilder addJSONFieldAccessor() {
        this.fieldAccessor = new FieldAccessor() {
            @Override
            public String genFieldAccessor(final String obj, final VirtFieldRef f) {
                if (DataSetBuilder.isRowAsField(f)) {
                    return obj;
                }
                return "getJsonObjectField(" + obj + ", \"" + f.getName() + "\", " + f.getTypeName() + ".class)";
            }
            
            @Override
            public String toString() {
                return "json getJsonObjectField() field accessor";
            }
        };
        return this;
    }
    
    DataSetBuilder addWAContext2RowAccessor() {
        this.rowAccessor = new RowAccessor() {
            @Override
            public String genRowAccessor(final String obj) {
                return "getWARowContext(" + obj + ", " + DataSetBuilder.this.dsType.getCanonicalName() + ".class)";
            }
            
            @Override
            public String toString() {
                return "getWARowContext(" + DataSetBuilder.this.id + ") row accessor";
            }
        };
        return this;
    }
    
    DataSet build() {
        if (this.rowAccessor == null) {
            this.rowAccessor = new RowAccessor() {
                @Override
                public String genRowAccessor(final String obj) {
                    return obj;
                }
                
                @Override
                public String toString() {
                    return "getRowData(" + DataSetBuilder.this.id + ") row accessor";
                }
            };
        }
        if (this.fullName == null || this.fieldAccessor == null || this.isStateful == null || this.fieldFactory == null || (!this.nested && this.uuid == null)) {
            throw new RuntimeException("Not all parameters set to build DataSet");
        }
        return new DataSet(this.id, this.dsName, this.dsType, this.kind, this.fullName, this.isStateful, this.uuid, this.fieldAccessor, this.rowAccessor, this.fieldFactory, this.virtFields, this.indexInfo, this.iterInfo, this.transSchemaInfo, this.jsonWAStoreInfo);
    }
}
