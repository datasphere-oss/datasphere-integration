package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IDataRecord
{
    void setTimestamp(final long p0) throws TransactionManagerException;
    
    void setLSN(final byte[] p0) throws TransactionManagerException;
    
    void setTransactionId(final byte[] p0) throws TransactionManagerException;
    
    void setMetaRecordID(final String p0) throws TransactionManagerException;
    
    void setOperationType(final operationtype p0) throws TransactionManagerException;
    
    void setColumnCount(final int p0) throws TransactionManagerException;
    
    void setColumnValueBefore(final int p0, final Object p1, final columntype p2) throws TransactionManagerException;
    
    void setColumnValueAsStringBefore(final int p0, final String p1) throws TransactionManagerException;
    
    void setColumnValueAsNullBefore(final int p0) throws TransactionManagerException;
    
    void setColumnValueAfter(final int p0, final Object p1, final columntype p2) throws TransactionManagerException;
    
    void setColumnValueAsStringAfter(final int p0, final String p1) throws TransactionManagerException;
    
    void setColumnValueAsNullAfter(final int p0) throws TransactionManagerException;
    
    void setParameterValue(final String p0, final String p1) throws TransactionManagerException;
}
