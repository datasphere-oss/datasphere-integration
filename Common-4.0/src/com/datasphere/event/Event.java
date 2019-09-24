package com.datasphere.event;

import java.util.Map;

import com.datasphere.uuid.UUID;

public abstract class Event
{
    public abstract UUID get_da_SimpleEvent_ID();
    
    public void setID(final UUID ID) {
    }
    
    public abstract long getTimeStamp();
    
    public void setTimeStamp(final long timeStamp) {
    }
    // 设置数据的净荷值
    public abstract void setPayload(final Object[] p0);
    
    public abstract Object[] getPayload();
    
    public abstract String getKey();
    
    public abstract void setKey(final String p0);
    
    public boolean setFromContextMap(final Map<String, Object> map) {
        return false;
    }
}
