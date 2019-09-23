package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.utils.*;
import com.datasphere.runtime.*;
import java.util.*;
import com.datasphere.runtime.exceptions.*;
import com.datasphere.runtime.compiler.exprs.*;

public class DataSets implements Iterable<DataSet>
{
    private final Map<String, DataSet> dataSets;
    private final Map<Integer, DataSet> dataSetsById;
    private final Map<String, List<DataSet>> fieldIndex;
    
    public DataSets() {
        this.dataSets = Factory.makeNameLinkedMap();
        this.dataSetsById = Factory.makeMap();
        this.fieldIndex = Factory.makeNameMap();
    }
    
    public int size() {
        return this.dataSets.size();
    }
    
    public boolean add(final DataSet ds) {
        final String dsname = ds.getName();
        if (this.dataSets.containsKey(dsname)) {
            return false;
        }
        this.dataSets.put(dsname, ds);
        final DataSet xds = this.dataSetsById.put(ds.getID(), ds);
        assert xds == null : "dataset id duplication";
        for (final Pair<String, Class<?>> p : ds.getAllTypeFields()) {
            final String fieldName = p.first;
            if (this.fieldIndex.containsKey(fieldName)) {
                this.fieldIndex.get(fieldName).add(ds);
            }
            else {
                final List<DataSet> value = new LinkedList<DataSet>();
                value.add(ds);
                this.fieldIndex.put(fieldName, value);
            }
        }
        return true;
    }
    
    public DataSet get(final int id) {
        final DataSet ds = this.dataSetsById.get(id);
        assert ds != null : "invalid dataset index";
        return ds;
    }
    
    public DataSet get(final String name) {
        return this.dataSets.get(name);
    }
    
    public FieldRef findField(final String name) throws AmbiguousFieldNameException {
        final List<DataSet> dssets = this.fieldIndex.get(name);
        if (dssets != null) {
            if (dssets.size() != 1) {
                throw new AmbiguousFieldNameException();
            }
            try {
                final DataSet ds = dssets.get(0);
                final DataSetRef dsref = ds.makeRef();
                return dsref.getField(name);
            }
            catch (NoSuchFieldException ex) {}
            catch (SecurityException ex2) {}
        }
        return null;
    }
    
    @Override
    public Iterator<DataSet> iterator() {
        return this.dataSets.values().iterator();
    }
}
