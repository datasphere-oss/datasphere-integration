package com.datasphere.source.lib.meta;

import java.nio.*;

import com.datasphere.source.lib.constant.*;

public class LEStringColumn extends Column
{
    public LEStringColumn() {
        this.setType(Constant.fieldType.STRING);
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int stringColumnLength) {
        final byte[] strLength = new byte[stringColumnLength];
        int i = 0;
        long accum = 0L;
        for (i = 0; i < stringColumnLength; ++i) {
            strLength[i] = buffer.get();
        }
        i = 0;
        for (int shiftBy = 0; shiftBy < stringColumnLength * 8; shiftBy += 8) {
            accum |= (strLength[i] & 0xFF) << shiftBy;
            ++i;
        }
        this.setSize((int)accum);
        return (int)accum;
    }
    
    @Override
    public Object getValue(final byte[] rowdata, final int offset, final int length) {
        final byte[] destination = new byte[length];
        System.arraycopy(rowdata, offset, destination, 0, length);
        return new String(destination);
    }
    
    @Override
    public Column clone() {
        return new LEStringColumn();
    }
}
