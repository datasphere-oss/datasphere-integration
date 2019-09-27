package com.datasphere.source.lib.meta;

import com.datasphere.source.lib.constant.*;

public class ByteColumn extends Column
{
    public ByteColumn() {
        this.setType(Constant.fieldType.BYTE);
    }
    
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        accum |= (rowData[offset] & 0xFF) << 0;
        return (byte)accum;
    }
    
    public static byte getByteValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        accum |= (rowData[offset] & 0xFF) << 0;
        return (byte)accum;
    }
    
    @Override
    public Column clone() {
        return new ByteColumn();
    }
}
