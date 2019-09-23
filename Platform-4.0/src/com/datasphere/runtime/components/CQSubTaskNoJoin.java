package com.datasphere.runtime.components;

import com.datasphere.recovery.*;
import com.datasphere.runtime.containers.*;

public abstract class CQSubTaskNoJoin extends CQSubTask
{
    private DARecord waRows;
    
    @Override
    public void init(final CQTask context) {
        this.context = context;
    }
    
    @Override
    public void setRow(final DARecord event) {
        this.waRows = event;
    }
    
    @Override
    public Object getRowData(final int ds) {
        assert ds == 0;
        final Object row = this.waRows.data;
        if (row == null) {
            final String streamName = this.context.getSourceName(ds);
            throw new RuntimeException("null data in stream <" + streamName + ">");
        }
        return row;
    }
    
    @Override
    public Position getRowPosition(final int ds) {
        assert ds == 0;
        return (this.waRows == null) ? null : this.waRows.position;
    }
    
    @Override
    public Object getWindowRow(final int ds, final int index) {
        return this.getRowData(ds);
    }
    
    @Override
    public Object[] getSourceEvents() {
        return new Object[] { this.waRows.data };
    }
    
    @Override
    public void updateState() {
        final IRange[] copyState;
        final IRange[] state = copyState = this.context.copyState();
        for (final IRange range : copyState) {
            if (range != null) {
                range.beginTransaction();
            }
        }
    }
    
    @Override
    public void cleanState() {
        final IRange[] copyState;
        final IRange[] state = copyState = this.context.copyState();
        for (final IRange range : copyState) {
            if (range != null) {
                range.endTransaction();
            }
        }
    }
}
