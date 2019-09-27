package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IStatusRecord
{
    void setname(final String p0) throws TransactionManagerException;
    
    void setIntegerMessage(final int p0) throws TransactionManagerException;
    
    void setStatusType(final recordstatus p0) throws TransactionManagerException;
    
    void setParameterValue(final String p0, final String p1) throws TransactionManagerException;
}
