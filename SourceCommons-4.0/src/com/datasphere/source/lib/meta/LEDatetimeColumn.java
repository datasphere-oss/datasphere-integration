package com.datasphere.source.lib.meta;

import java.text.*;
import java.sql.*;
import java.nio.*;

public class LEDatetimeColumn extends Column
{
    private SimpleDateFormat ft;
    
    public LEDatetimeColumn() {
        this.ft = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
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
        final Date dt = new Date(accum);
        return this.ft.format(dt);
    }
    
    @Override
    public int getLengthOfString(final ByteBuffer buffer, final int i) {
        return 0;
    }
    
    @Override
    public Column clone() {
        return new LEDatetimeColumn();
    }
}
