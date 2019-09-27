package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class Column
{
    int index;
    String name;
    Constant.fieldType type;
    int size;
    int lengthOffset;
    
    public Column() {
        this.index = -1;
    }
    
    public int getIndex() {
        return this.index;
    }
    
    public void setIndex(final int index) {
        this.index = index;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public Constant.fieldType getType() {
        return this.type;
    }
    
    public void setType(final Constant.fieldType type) {
        this.type = type;
    }
    
    public int getSize() {
        return this.size;
    }
    
    public void setSize(final int size) {
        this.size = size;
    }
    
    public void lengthOffset(final int noOfBytes) {
        this.lengthOffset = noOfBytes;
    }
    
    public int lengthOffset() {
        return this.lengthOffset;
    }
    
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        return "";
    }
    
    public int getLengthOfString(final ByteBuffer buffer, final int stringColumnLength) {
        return -1;
    }
    
    public Column clone() {
        return new Column();
    }
}
