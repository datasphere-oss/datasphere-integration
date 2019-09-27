package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.constant.*;

public class LEDoubleColumn extends Column
{
    public LEDoubleColumn() {
        this.setType(Constant.fieldType.DOUBLE);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 0;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            ++i;
        }
        final double retval = Double.longBitsToDouble(accum);
        return retval;
    }
    
    @Override
    public Column clone() {
        return new LEDoubleColumn();
    }
}
