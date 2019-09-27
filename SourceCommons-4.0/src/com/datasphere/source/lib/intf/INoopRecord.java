package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;

public interface INoopRecord
{
    void setTimestamp(final long p0) throws TransactionManagerException;
    
    void setLSN(final byte[] p0) throws TransactionManagerException;
}
