package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class LEIntegerColumn extends Column
{
    public LEIntegerColumn() {
        this.setType(Constant.fieldType.INTEGER);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 0;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            ++i;
        }
        return (int)accum;
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int i) {
        return 0;
    }
    
    @Override
    public Column clone() {
        return new LEIntegerColumn();
    }
}
