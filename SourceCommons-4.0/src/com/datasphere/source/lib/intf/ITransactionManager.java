package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface ITransactionManager
{
    void init(final ISession p0) throws TransactionManagerException;
    
    String getNextMetadataRecordID() throws TransactionManagerException;
    
    boolean getMetadataRecord(final String p0, final IMetadataRecord p1) throws TransactionManagerException;
    
    recordstatus getRecord(final IRecord p0) throws TransactionManagerException;
    
    void acknowledge(final acktype p0, final IPosition p1) throws TransactionManagerException;
    
    void close() throws TransactionManagerException;
}
