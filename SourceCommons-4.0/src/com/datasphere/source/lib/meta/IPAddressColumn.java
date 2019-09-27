package com.datasphere.source.lib.meta;

import org.apache.log4j.*;

import com.datasphere.source.lib.constant.*;

import java.net.*;

public class IPAddressColumn extends Column
{
    private static Logger logger;
    
    public IPAddressColumn() {
        this.setType(Constant.fieldType.IP);
    }
    
    @Override
    public Object getValue(final byte[] data, final int offset, final int length) {
        final byte[] addressArray = new byte[length];
        System.arraycopy(data, offset, addressArray, 0, length);
        InetAddress ipAddress = null;
        try {
            ipAddress = InetAddress.getByAddress(addressArray);
        }
        catch (UnknownHostException e) {
            IPAddressColumn.logger.warn((Object)"Unable to parse Ip Address from the given byte array");
        }
        return ipAddress.getHostAddress();
    }
    
    @Override
    public Column clone() {
        return new IPAddressColumn();
    }
    
    static {
        IPAddressColumn.logger = Logger.getLogger((Class)IPAddressColumn.class);
    }
}
