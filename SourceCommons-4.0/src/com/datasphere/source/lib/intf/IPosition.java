package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

public interface IPosition
{
    positiontype getPositionType() throws TransactionManagerException;
    
    byte[] getPositionValue() throws TransactionManagerException;
}
