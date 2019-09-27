package com.datasphere.source.lib.meta;

import org.apache.log4j.*;

import com.datasphere.source.lib.constant.*;

public class MacIDColumn extends Column
{
    private static Logger logger;
    
    public MacIDColumn() {
        this.setType(Constant.fieldType.MACID);
    }
    
    @Override
    public Object getValue(final byte[] data, final int offset, final int length) {
        final byte[] addressArray = new byte[length];
        System.arraycopy(data, offset, addressArray, 0, length);
        String str = "";
        str += String.format("%02X", addressArray[0] & 0xFF);
        for (int itr = 1; itr < length; ++itr) {
            str = str + "-" + String.format("%02X", addressArray[itr] & 0xFF);
        }
        return str;
    }
    
    @Override
    public Column clone() {
        return new MacIDColumn();
    }
    
    static {
        MacIDColumn.logger = Logger.getLogger((Class)MacIDColumn.class);
    }
}
