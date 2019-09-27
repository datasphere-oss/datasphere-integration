package com.datasphere.source.lib.meta;

import java.nio.*;

public class StringColumnNullTerminated extends Column
{
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int stringColumnLength) {
        int len = 0;
        for (byte byte0 = buffer.get(); byte0 != 0; byte0 = buffer.get()) {
            ++len;
        }
        this.setSize(len);
        return len;
    }
    
    @Override
    public Object getValue(final byte[] rowdata, final int offset, final int length) {
        final byte[] destination = new byte[length];
        System.arraycopy(rowdata, offset, destination, 0, length);
        return new String(destination);
    }
    
    @Override
    public Column clone() {
        return new StringColumnNullTerminated();
    }
}
