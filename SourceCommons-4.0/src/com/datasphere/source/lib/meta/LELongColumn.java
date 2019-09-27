package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class LELongColumn extends Column
{
    public LELongColumn() {
        this.setType(Constant.fieldType.LONG);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length) {
            return -1;
        }
        long accum = 0L;
        int i = 0;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            ++i;
        }
        return accum;
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int i) {
        return 0;
    }
    
    @Override
    public Column clone() {
        return new LELongColumn();
    }
}
