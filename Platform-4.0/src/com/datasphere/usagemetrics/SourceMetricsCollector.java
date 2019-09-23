package com.datasphere.usagemetrics;

import com.datasphere.recovery.*;
import com.datasphere.uuid.*;

public interface SourceMetricsCollector
{
    void setSourcePosition(final SourcePosition p0, final UUID p1);
    
    void addSourceBytes(final long p0, final UUID p1);
    
    void flush();
}
