package com.datasphere.runtime.components;

import com.datasphere.runtime.containers.*;
import java.io.*;
import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.runtime.*;

public abstract class CQSubTaskJoin extends CQSubTask
{
    private DARecord[] rows;
    private Iterator<DARecord>[] iterators;
    private IRange[] state;
    
    public boolean isUpdateIndexesCodeGenerated() {
        return true;
    }
    
    private void set(final int index, final DARecord r) {
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("set(" + index + ", " + r + ")");
        }
        this.rows[index] = r;
    }
    
    @Override
    public void init(final CQTask context) {
        this.context = context;
        final int dscount = context.getDataSetCount();
        this.rows = new DARecord[dscount];
        this.iterators = (Iterator<DARecord>[])new Iterator[dscount];
    }
    
    @Override
    public void updateState() {
        this.state = this.context.copyState();
        for (final IRange range : this.state) {
            range.beginTransaction();
        }
        final boolean isUpdateIndexesCodeGenerated = this.isUpdateIndexesCodeGenerated();
        if (isUpdateIndexesCodeGenerated) {
        		Iterator addIter = this.getAdded().iterator();
        		Iterator removeIter = this.getRemoved().iterator();
        		
            while (addIter.hasNext()) {
            		final DARecord e = (DARecord)addIter.next();
                this.updateIndexes(e, true);
            }
            
            while (removeIter.hasNext()) {
            		final DARecord e = (DARecord)removeIter.next();
                this.updateIndexes(e, false);
            }
        }
    }
    
    @Override
    public void cleanState() {
        for (final IRange range : this.state) {
            range.endTransaction();
        }
    }
    
    @Override
    public Object getRowData(final int ds) {
        final Object row = (this.rows[ds] == null) ? null : this.rows[ds].data;
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("getRowData(" + ds + ") returned " + row);
        }
        return row;
    }
    
    @Override
    public Position getRowPosition(final int ds) {
        final Position row = (this.rows[ds] == null) ? null : this.rows[ds].position;
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("getRowPosition(" + ds + ") returned " + row);
        }
        return row;
    }
    
    @Override
    public Object getWindowRow(final int ds, final int index) {
        final Object row = this.getRowData(ds);
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("getWindowRow(" + ds + ", " + index + ") returned " + row);
        }
        return row;
    }
    
    public void createEmptyIterator(final int ds) {
        this.iterators[ds] = Collections.emptyIterator();
    }
    
    public void createWindowIterator(final int ds) {
        final IRange rng = this.state[ds];
        assert rng != null;
        Iterator<DARecord> i = (Iterator<DARecord>)rng.all().iterator();
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("createWindowIterator(" + ds + ")");
            while (i.hasNext()) {
                out.println("  " + i.next());
            }
            i = (Iterator<DARecord>)rng.all().iterator();
        }
        this.iterators[ds] = i;
    }
    
    public void createIndexIterator(final int ds, final int index, final RecordKey key) {
        this.iterators[ds] = this.context.createIndexIterator(index, key);
    }
    
    public boolean fetch(final int ds) {
        final Iterator<DARecord> it = this.iterators[ds];
        final boolean ret = it.hasNext();
        final DARecord e = ret ? it.next() : null;
        this.set(ds, e);
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("fetch(" + ds + ") returned " + ret);
        }
        return ret;
    }
    
    public void updateIndexes(final DARecord event, final boolean doadd) {
    }
    
    @Override
    public void setRow(final DARecord event) {
        if (this.context.isTracingExecution()) {
            final PrintStream out = this.context.getTracingStream();
            out.println("setRow(" + event + ")");
        }
        this.set(this.getThisDS(), event);
    }
    
    public void createWindowLookupIterator(final int ds, final int index, final RecordKey key) {
        this.iterators[ds] = (Iterator<DARecord>)this.state[ds].lookup(index, key).iterator();
    }
    
    @Override
    public Object[] getSourceEvents() {
        final Object[] result = new Object[this.rows.length];
        for (int i = 0; i < this.rows.length; ++i) {
            result[i] = this.rows[i].data;
        }
        return result;
    }
    
    public void createCollectionIterator(final int ds, final Object o) {
        this.iterators[ds] = new Iterator<DARecord>() {
            final Iterator<Object> it = ((Iterable)o).iterator();
            
            @Override
            public boolean hasNext() {
                return this.it.hasNext();
            }
            
            @Override
            public DARecord next() {
                return new DARecord(this.it.next());
            }
            
            @Override
            public void remove() {
            }
        };
    }
}
