package com.datasphere.runtime.compiler.select;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.uuid.UUID;

public class DataSet extends RowSet
{
    private final int id;
    private final String name;
    private final Kind kind;
    private final String fullName;
    private final boolean isStateful;
    private final UUID uuid;
    private final IndexInfo indexInfo;
    private final IteratorInfo iterInfo;
    private final TraslatedSchemaInfo transSchemaInfo;
    private final JsonWAStoreInfo jsonWAStoreInfo;
    
    protected DataSet(final int id, final String name, final Class<?> type, final Kind kind, final String fullName, final boolean isStateful, final UUID uuid, final FieldAccessor fieldAccessor, final RowAccessor rowAccessor, final FieldFactory fieldFactory, final List<Pair<String, Class<?>>> virtFields, final IndexInfo indexInfo, final IteratorInfo iterInfo, final TraslatedSchemaInfo transSchemaInfo, final JsonWAStoreInfo jsonWAStoreInfo) {
        super(type, fieldAccessor, rowAccessor, fieldFactory, virtFields);
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.fullName = fullName;
        this.isStateful = isStateful;
        this.uuid = uuid;
        this.indexInfo = indexInfo;
        this.iterInfo = iterInfo;
        this.transSchemaInfo = transSchemaInfo;
        this.jsonWAStoreInfo = jsonWAStoreInfo;
    }
    
    public final int getID() {
        return this.id;
    }
    
    public final String getName() {
        return this.name;
    }
    
    public final Kind getKind() {
        return this.kind;
    }
    
    public final BitSet id2bitset() {
        final BitSet bs = new BitSet();
        bs.set(this.id);
        return bs;
    }
    
    public final DataSetRef makeRef() {
        return DataSetRef.makeDataSetRef(this);
    }
    
    @Override
    public String toString() {
        return "DataSet<" + this.kind + ">(" + this.id + ", " + this.getFullName() + ", alias " + this.name + ", " + this.rsToString() + ")";
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataSet)) {
            return false;
        }
        final DataSet other = (DataSet)o;
        return this.id == other.id;
    }
    
    @Override
    public int hashCode() {
        return this.id;
    }
    
    public boolean isStateful() {
        return this.isStateful;
    }
    
    public UUID getUUID() {
        if (this.uuid == null) {
            throw new UnsupportedOperationException();
        }
        return this.uuid;
    }
    
    public String getFullName() {
        return this.fullName;
    }
    
    public TraslatedSchemaInfo isTranslated() {
        return this.transSchemaInfo;
    }
    
    public IteratorInfo isIterator() {
        return this.iterInfo;
    }
    
    public JsonWAStoreInfo isJsonWAStore() {
        return this.jsonWAStoreInfo;
    }
    
    public List<FieldRef> makeListOfAllFields() {
        return this.makeListOfAllFields(this.makeRef());
    }
    
    public List<Integer> getListOfIndexesForFields(final Set<String> set) {
        return (this.indexInfo == null) ? Collections.emptyList() : this.indexInfo.getListOfIndexesForFields(set);
    }
    
    public List<String> indexFieldList(final int idxID) {
        return (this.indexInfo == null) ? Collections.emptyList() : this.indexInfo.indexFieldList(idxID);
    }
    
    public enum Kind
    {
        WINDOW_JAVA_OBJECTS, 
        CACHE_JAVA_OBJECTS, 
        STREAM_JAVA_OBJECTS, 
        STREAM_GENERATOR, 
        SELECT_VIEW, 
        TRANSLATED_DYNAMIC, 
        WASTORE_JSON_TYPED, 
        WASTORE_JAVA_OBJECTS, 
        NESTED_JAVA_UNSTRUCT, 
        NESTED_JSON_UNSTRUCT, 
        NESTED_JAVA_OBJECTS, 
        NESTED_JSON_TYPED;
    }
}
