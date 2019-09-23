package com.datasphere.intf;

import com.datasphere.uuid.*;

public interface DashboardOperations
{
    String exportDashboard(final AuthToken p0, final String p1) throws Exception;
    
    UUID importDashboard(final AuthToken p0, final Boolean p1, final String p2, final String p3) throws Exception;
    
    UUID importDashboard(final AuthToken p0, final String p1, final Boolean p2, final String p3, final String p4) throws Exception;
}
