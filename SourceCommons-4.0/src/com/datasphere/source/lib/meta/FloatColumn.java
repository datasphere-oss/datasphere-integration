package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.constant.*;

public class FloatColumn extends Column
{
    public FloatColumn() {
        this.setType(Constant.fieldType.FLOAT);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 3;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        final int ret = (int)accum;
        final float retVal = Float.intBitsToFloat(ret);
        return retVal;
    }
    
    public static float getFloatValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1.0f;
        }
        long accum = 0L;
        int i = 3;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            accum |= (rowData[offset + i] & 0xFF) << shiftBy;
            --i;
        }
        final int ret = (int)accum;
        final float retVal = Float.intBitsToFloat(ret);
        return retVal;
    }
    
    @Override
    public Column clone() {
        return new FloatColumn();
    }
}
