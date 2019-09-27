package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IRecord
{
    void setRecordType(final recordtype p0) throws TransactionManagerException;
    
    IControlRecord getControlRecord() throws TransactionManagerException;
    
    IDataRecord getDataRecord() throws TransactionManagerException;
    
    void readFromBytes(final byte[] p0) throws TransactionManagerException;
    
    IDDLRecord getCDCDDLRecord() throws TransactionManagerException;
}
