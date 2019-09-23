package com.datasphere.runtime.compiler.select;

import java.util.List;

import com.datasphere.event.SimpleEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.containers.DynamicEventWrapper;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class DataSetFactory
{
    private static String makeName(final String name, final String alias) {
        return (alias == null) ? name : alias;
    }
    
    private static DataSetBuilder createJavaObjectDS(final int id, final DataSet.Kind kind, final String name, final String alias, final Class<?> type, final MetaInfo.MetaObject si, final boolean isStateful) {
        return new DataSetBuilder(id, kind, makeName(name, alias), type).addJavatypeFields(type).addStateful_UUID_FullName(isStateful, si.uuid, si.getFullName());
    }
    
    public static DataSet createWindowDS(final int id, final String name, final String alias, final Class<?> type, final MetaInfo.Window wi) {
        return createJavaObjectDS(id, DataSet.Kind.WINDOW_JAVA_OBJECTS, name, alias, type, wi, true).addWindowIndexInfo(wi).build();
    }
    
    public static DataSet createCacheDS(final int id, final String name, final String alias, final Class<?> type, final MetaInfo.Cache ci) {
        return createJavaObjectDS(id, DataSet.Kind.CACHE_JAVA_OBJECTS, name, alias, type, ci, true).addCacheIndexInfo(ci).build();
    }
    
    public static DataSet createStreamDS(final int id, final String name, final String alias, final Class<?> type, final MetaInfo.Stream si) {
        return createJavaObjectDS(id, DataSet.Kind.STREAM_JAVA_OBJECTS, name, alias, type, si, false).build();
    }
    
    public static DataSet createStreamFunctionDS(final int id, final String name, final Class<?> type, final MetaInfo.StreamGenerator gi) {
        return createJavaObjectDS(id, DataSet.Kind.STREAM_GENERATOR, name, name, type, gi, false).build();
    }
    
    public static DataSet createSelectViewDS(final int id, final String alias, final UUID selectId, final List<RSFieldDesc> resultSetDesc, final boolean isStateful) {
        return new DataSetBuilder(id, DataSet.Kind.SELECT_VIEW, alias, SimpleEvent.class).addRSDescFields(resultSetDesc).addStateful_UUID_FullName(isStateful, selectId, alias).addPayloadFieldAccessor().build();
    }
    
    public static DataSet createTranslatedDS(final int id, final String name, final String alias, final List<RSFieldDesc> resultSetDesc, final UUID expectedTypeID, final MetaInfo.MetaObject si) {
        return new DataSetBuilder(id, DataSet.Kind.TRANSLATED_DYNAMIC, makeName(name, alias), DynamicEventWrapper.class).addRSDescFields(resultSetDesc).addStateful_UUID_FullName(false, si.uuid, si.getFullName()).addSchemaTranslation(expectedTypeID).build();
    }
    
    public static DataSet createJSONWAStoreDS(final int id, final String name, final String alias, final List<RSFieldDesc> fields, final MetaInfo.WAStoreView view, final MetaInfo.HDStore store) {
        return new DataSetBuilder(id, DataSet.Kind.WASTORE_JSON_TYPED, makeName(name, alias), JsonNodeEvent.class).addRSDescFields(fields).addStateful_UUID_FullName(true, view.uuid, store.getFullName()).addJsonWAStoreInfo(view, store).build();
    }
    
    public static DataSet createWAStoreDS(final int id, final String name, final String alias, final Class<?> type, final MetaInfo.WAStoreView view, final MetaInfo.HDStore store) {
        if (store.usesOldHDStore()) {
            return createJavaObjectDS(id, DataSet.Kind.WASTORE_JAVA_OBJECTS, name, alias, type, view, true).addWAContext2RowAccessor().build();
        }
        return createJSONWAStoreDS(id, name, alias, DataSetBuilder.makeRSDesc(type), view, store);
    }
    
    public static DataSet createNestedJavaReflect(final int id, final DataSet parent, final FieldRef parentField, final String alias) {
        return new DataSetBuilder(id, DataSet.Kind.NESTED_JAVA_UNSTRUCT, alias, Object.class).addIteratorInfo(parent, parentField).addUnstructredFields(Object.class, Object.class).addJavaReflectFieldAccessor().build();
    }
    
    public static DataSet createNestedJsonUnstruct(final int id, final DataSet parent, final FieldRef parentField, final String alias) {
        return new DataSetBuilder(id, DataSet.Kind.NESTED_JSON_UNSTRUCT, alias, JsonNode.class).addIteratorInfo(parent, parentField).addUnstructredFields(JsonNode.class, JsonNode.class).addJSONFieldAccessor().build();
    }
    
    public static DataSet createNestedJavaObjects(final int id, final DataSet parent, final FieldRef parentField, final String alias, final Class<?> itemType) {
        return new DataSetBuilder(id, DataSet.Kind.NESTED_JAVA_OBJECTS, alias, itemType).addIteratorInfo(parent, parentField).addJavatypeFields(itemType).build();
    }
    
    public static DataSet createNestedJsonTyped(final int id, final DataSet parent, final FieldRef parentField, final String alias, final List<RSFieldDesc> fields) {
        return new DataSetBuilder(id, DataSet.Kind.NESTED_JSON_TYPED, alias, JsonNode.class).addIteratorInfo(parent, parentField).addRSDescFields(fields).addJSONFieldAccessor().build();
    }
}
