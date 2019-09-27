package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;

public interface TransactionManagerFactory
{
    ITransactionManager createInstance() throws TransactionManagerException;
    
    void destroyInstance() throws TransactionManagerException;
}
