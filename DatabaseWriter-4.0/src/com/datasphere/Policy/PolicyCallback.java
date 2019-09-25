package com.datasphere.Policy;

import com.datasphere.recovery.*;
import com.datasphere.event.*;
import java.sql.*;

public interface PolicyCallback
{
    void updateConsolidatedPosition(final Position p0);
    
    boolean onError(final Event p0, final Exception p1);
    
    void reset();
    
    Connection getConnection();
}
