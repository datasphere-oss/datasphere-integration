package com.datasphere.source.lib.meta;

import java.math.*;

public class BigIntegerColumn extends Column
{
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        if (rowData.length < offset + length || length < 0) {
            return new BigInteger("-1");
        }
        BigInteger bigInteger = new BigInteger("0");
        for (int itr = 0; itr < length; ++itr) {
            final Short shortValue = (short)(rowData[offset + itr] & 0xFF);
            bigInteger = bigInteger.shiftLeft(8);
            bigInteger = bigInteger.add(BigInteger.valueOf(shortValue));
        }
        return bigInteger;
    }
    
    @Override
    public Column clone() {
        return new BigIntegerColumn();
    }
}
