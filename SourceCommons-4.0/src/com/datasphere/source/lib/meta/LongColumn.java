package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class LongColumn extends Column
{
    public LongColumn() {
        this.setType(Constant.fieldType.LONG);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length) {
            return -1;
        }
        long accum = 0L;
        for (int i = length - 1, shiftBy = 0; shiftBy < 64 && i >= 0; --i, shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
        }
        return accum;
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int i) {
        return 0;
    }
    
    public static long getLongValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length) {
            return -1L;
        }
        long accum = 0L;
        int i = 7;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        return accum;
    }
    
    @Override
    public Column clone() {
        return new LongColumn();
    }
}
