package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class ShortColumn extends Column
{
    public ShortColumn() {
        this.setType(Constant.fieldType.SHORT);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length) {
            return -1;
        }
        long accum = 0L;
        for (int i = length - 1, shiftBy = 0; shiftBy < 16 && i >= 0; --i, shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
        }
        return (short)accum;
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int i) {
        return 0;
    }
    
    @Override
    public Column clone() {
        return new ShortColumn();
    }
}
