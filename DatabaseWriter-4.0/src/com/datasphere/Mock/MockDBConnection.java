package com.datasphere.Mock;

import com.datasphere.Connection.*;
import com.datasphere.exception.*;

public class MockDBConnection extends DatabaseWriterConnection
{
    public MockDBConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        super(dbUrl, dbUser, dbPasswd);
    }
    
    @Override
    public void connect(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
    }
    
    public void setAutoCommit(final boolean flag) throws DatabaseWriterException {
    }
}
