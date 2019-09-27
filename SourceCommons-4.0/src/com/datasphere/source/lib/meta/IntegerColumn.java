package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.constant.*;

public class IntegerColumn extends Column
{
    public IntegerColumn() {
        this.setType(Constant.fieldType.INTEGER);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length) {
            return -1;
        }
        long accum = 0L;
        for (int i = length - 1, shiftBy = 0; shiftBy < 32 && i >= 0; --i, shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
        }
        return (int)accum;
    }
    
    public static int getIntValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 3;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        return (int)accum;
    }
    
    @Override
    public Column clone() {
        return new IntegerColumn();
    }
}
