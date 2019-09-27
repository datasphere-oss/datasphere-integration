package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IMetadataRecord
{
    void setTimestamp(final long p0) throws TransactionManagerException;
    
    void setLSN(final byte[] p0) throws TransactionManagerException;
    
    void setTransactionId(final String p0) throws TransactionManagerException;
    
    void setMetadataRecordId(final String p0) throws TransactionManagerException;
    
    void setVersion(final String p0) throws TransactionManagerException;
    
    void setSchemaName(final String p0) throws TransactionManagerException;
    
    void setDatabaseName(final String p0) throws TransactionManagerException;
    
    void setTableName(final String p0) throws TransactionManagerException;
    
    void setColumnCount(final int p0) throws TransactionManagerException;
    
    void setColumnType(final columntype p0, final int p1) throws TransactionManagerException;
    
    void setColumnIsNullable(final int p0) throws TransactionManagerException;
    
    void setKeyColumn(final int p0) throws TransactionManagerException;
    
    void setColumnPrecision(final int p0, final int p1) throws TransactionManagerException;
    
    void setColumnScale(final int p0, final int p1) throws TransactionManagerException;
    
    void setColumnName(final String p0, final int p1) throws TransactionManagerException;
    
    void setColumnSize(final int p0, final int p1) throws TransactionManagerException;
}
