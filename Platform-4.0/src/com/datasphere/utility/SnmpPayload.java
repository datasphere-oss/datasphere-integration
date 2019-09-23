package com.datasphere.utility;

public class SnmpPayload
{
    byte[] snmpBytes;
    String snmpString;
    
    public SnmpPayload(final byte[] b, final String str) {
        this.snmpBytes = (byte[])((b != null) ? ((byte[])b.clone()) : null);
        this.snmpString = str;
    }
    
    public String getSnmpStringValue() {
        return this.snmpString;
    }
    
    public byte[] getSnmpByteValue() {
        return this.snmpBytes;
    }
}
