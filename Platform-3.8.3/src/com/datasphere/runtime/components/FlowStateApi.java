package com.datasphere.runtime.components;

import java.util.List;

import com.datasphere.uuid.UUID;

public interface FlowStateApi
{
    void startFlow(final List<UUID> p0, final Long p1) throws Exception;
    
    void startFlow(final Long p0) throws Exception;
    
    void stopFlow(final Long p0) throws Exception;
    
    void pauseSources(final Long p0) throws Exception;
    
    void approveQuiesce(final Long p0) throws Exception;
    
    void unpauseSources(final Long p0) throws Exception;
    
    void checkpoint(final Long p0) throws Exception;
    
    void quiesceCheckpoint(final Long p0) throws Exception;
    
    void quiesceFlush(final Long p0) throws Exception;
    
    void memoryStatus() throws Exception;
    
    void startCaches(final List<UUID> p0, final Long p1) throws Exception;
    
    void stopCaches(final Long p0) throws Exception;
}
