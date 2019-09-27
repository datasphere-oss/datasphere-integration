package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.constant.*;

public class DoubleColumn extends Column
{
    public DoubleColumn() {
        this.setType(Constant.fieldType.DOUBLE);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 7;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        final double retval = Double.longBitsToDouble(accum);
        return retval;
    }
    
    public static Double getDoubleValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1.0;
        }
        long accum = 0L;
        int i = 7;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        final double retval = Double.longBitsToDouble(accum);
        return retval;
    }
    
    @Override
    public Column clone() {
        return new DoubleColumn();
    }
}
