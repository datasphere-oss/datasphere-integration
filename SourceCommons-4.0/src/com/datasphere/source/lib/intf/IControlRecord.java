package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IControlRecord
{
    void setTimestamp(final long p0) throws TransactionManagerException;
    
    void setLSN(final byte[] p0) throws TransactionManagerException;
    
    void setTransactionId(final byte[] p0) throws TransactionManagerException;
    
    void setControlType(final controltype p0) throws TransactionManagerException;
    
    void setTransactionUserId(final String p0) throws TransactionManagerException;
    
    void setCommand(final String p0) throws TransactionManagerException;
}
