package com.datasphere.runtime.compiler.select;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.exprs.VirtFieldRef;
import com.datasphere.runtime.utils.Factory;

public class DataSourceNestedCollection extends DataSourcePrimary
{
    private final String fieldName;
    private final String alias;
    private final TypeName itemTypeName;
    private final List<TypeField> expectedFieldsDesc;
    
    public DataSourceNestedCollection(final String fieldName, final String alias, final TypeName typeName, final List<TypeField> fields) {
        this.fieldName = fieldName;
        this.alias = alias;
        this.itemTypeName = typeName;
        this.expectedFieldsDesc = fields;
    }
    
    @Override
    public String toString() {
        return "ITERATOR(" + this.fieldName + ") AS " + this.alias;
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) {
        throw new UnsupportedOperationException();
    }
    
    private FieldRef getRowAsField(final DataSet ds) {
        final List<FieldRef> flds = ds.makeListOfAllFields();
        if (flds.size() != 1) {
            return null;
        }
        final FieldRef fld = flds.get(0);
        if (!(fld instanceof VirtFieldRef)) {
            return null;
        }
        if (!DataSetBuilder.isRowAsField((VirtFieldRef)fld)) {
            return null;
        }
        return fld;
    }
    
    private FieldRef resolveField(final DataSet ds, final String fieldName) {
        FieldRef fld = null;
        try {
            ValueExpr dsref = ds.makeRef();
            for (final String namePart : fieldName.split("\\.")) {
                fld = (FieldRef)(dsref = dsref.getField(namePart));
            }
        }
        catch (NoSuchFieldException | SecurityException ex2) {
            return null;
        }
        return fld;
    }
    
    @Override
    public DataSet resolveDataSource(final DataSource.Resolver r, final Map<String, DataSet> resolved, final boolean onlyPrimary) throws MetaDataRepositoryException {
        if (onlyPrimary) {
            return null;
        }
        final String dsAndFieldName = this.fieldName;
        final int pos = dsAndFieldName.indexOf(46);
        final String datasetName = (pos == -1) ? dsAndFieldName : dsAndFieldName.substring(0, pos);
        final String fieldName = (pos == -1) ? null : dsAndFieldName.substring(pos + 1);
        final DataSet ds = resolved.get(datasetName);
        if (ds == null) {
            return null;
        }
        final FieldRef fld = (fieldName != null) ? this.resolveField(ds, fieldName) : this.getRowAsField(ds);
        if (fld == null) {
            r.error("no such field", dsAndFieldName);
        }
        final Class<?> type = fld.getType();
        if (!Iterable.class.isAssignableFrom(type)) {
            r.error("has not iterable type", dsAndFieldName);
        }
        if (this.itemTypeName != null) {
            final Class<?> itemType = r.getTypeInfo(this.itemTypeName);
            return DataSetFactory.createNestedJavaObjects(r.getNextDataSetID(), ds, fld, this.alias, itemType);
        }
        if (this.expectedFieldsDesc != null) {
            final List<RSFieldDesc> flds = this.makeRSDesc(r, this.expectedFieldsDesc);
            return DataSetFactory.createNestedJsonTyped(r.getNextDataSetID(), ds, fld, this.alias, flds);
        }
        if (JsonNode.class.isAssignableFrom(type)) {
            return DataSetFactory.createNestedJsonUnstruct(r.getNextDataSetID(), ds, fld, this.alias);
        }
        return DataSetFactory.createNestedJavaReflect(r.getNextDataSetID(), ds, fld, this.alias);
    }
    
    private List<RSFieldDesc> makeRSDesc(final DataSource.Resolver r, final List<TypeField> fields) {
        final Set<String> names = Factory.makeNameSet();
        final List<RSFieldDesc> ret = new ArrayList<RSFieldDesc>();
        for (final TypeField tf : fields) {
            final String fieldName = tf.fieldName;
            if (names.contains(fieldName)) {
                r.error("duplicated field name", fieldName);
            }
            else {
                names.add(fieldName);
                final Class<?> type = r.getTypeInfo(tf.fieldType);
                ret.add(new RSFieldDesc(fieldName, type));
            }
        }
        return ret;
    }
}
