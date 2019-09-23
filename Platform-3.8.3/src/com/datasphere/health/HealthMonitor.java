package com.datasphere.health;

import com.datasphere.uuid.*;

public interface HealthMonitor
{
    HealthRecordCollection getHealthRecordsByCount(final int p0, final long p1) throws Exception;
    
    HealthRecordCollection getHealthRecordsByTime(final long p0, final long p1) throws Exception;
    
    HealthRecordCollection getHealtRecordById(final UUID p0) throws Exception;
    
    HealthRecordCollection getHealthRecordsWithIssuesList(final long p0, final long p1) throws Exception;
    
    HealthRecordCollection getHealthRecordsWithStatusChangeList(final long p0, final long p1) throws Exception;
}
