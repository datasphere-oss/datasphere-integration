package com.datasphere.usagemetrics.provider;

import com.datasphere.uuid.*;
import com.datasphere.recovery.*;
import com.datasphere.usagemetrics.metrics.*;

public interface Provider
{
    UUID getSourceID();
    
    SourcePosition getPosition();
    
    Metrics getMetrics();
}
