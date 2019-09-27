package com.datasphere.source.lib.intf;

import com.datasphere.source.lib.exc.*;
import com.datasphere.source.lib.type.*;

import java.util.*;

public interface ISession
{
    sessiontype getSessionType() throws TransactionManagerException;
    
    IPosition getPosition() throws TransactionManagerException;
    
    String[] getTableList() throws TransactionManagerException;
    
    String[] getExcludedTableList() throws TransactionManagerException;
    
    List<String> getKeyColumns(final String p0) throws TransactionManagerException;
    
    List<String> getFetchColumns(final String p0) throws TransactionManagerException;
    
    List<String> getExcColumns(final String p0) throws TransactionManagerException;
    
    List<String> getReplacedColumns(final String p0) throws TransactionManagerException;
    
    String[] getColumns(final String p0) throws TransactionManagerException;
    
    String[] getExcludedColumns(final String p0) throws TransactionManagerException;
    
    String getParameterValue(final String p0) throws TransactionManagerException;
    
    Map<String, Object> getPropertyMap() throws TransactionManagerException;
}
