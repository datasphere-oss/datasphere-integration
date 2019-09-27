package com.datasphere.source.lib.meta;

import java.util.*;

public class MetaData
{
    List<Column> columns;
    
    public MetaData(final Column[] cols) {
        this.init();
        for (int itr = 0; itr < cols.length; ++itr) {
            this.columns.add(cols[itr]);
        }
    }
    
    public void init() {
        this.columns = new ArrayList<Column>();
    }
    
    public MetaData(final String metaDataFile) {
    }
    
    public void addColumn(final Column col) {
        this.columns.add(col);
    }
    
    public void addColumn(final Column col, final int index) {
        this.columns.add(index, col);
    }
    
    public void clear() {
        this.columns.clear();
    }
    
    public Column[] colums() {
        final Column[] colList = new Column[this.columns.size()];
        for (int itr = 0; itr < this.columns.size(); ++itr) {
            colList[itr] = this.columns.get(itr);
        }
        return colList;
    }
    
    public void dump() {
        System.out.println("*********** Column List ***************");
        for (int itr = 0; itr < this.columns.size(); ++itr) {
            System.out.println("Column [" + itr + "] : [" + this.columns.get(itr).toString() + "]");
        }
        System.out.println("***************************************");
    }
}
