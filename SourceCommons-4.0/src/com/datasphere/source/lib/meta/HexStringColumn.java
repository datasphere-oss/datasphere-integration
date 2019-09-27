package com.datasphere.source.lib.meta;

import javax.xml.bind.*;

public class HexStringColumn extends Column
{
    @Override
    public Object getValue(final byte[] rowData, final int offset, final int length) {
        String retValue = "";
        if (rowData.length < offset + length || length < 0) {
            return retValue;
        }
        final byte[] valueBuffer = new byte[length];
        System.arraycopy(rowData, offset, valueBuffer, 0, length);
        retValue = "0x" + DatatypeConverter.printHexBinary(valueBuffer);
        return retValue;
    }
    
    @Override
    public Column clone() {
        return new HexStringColumn();
    }
}
