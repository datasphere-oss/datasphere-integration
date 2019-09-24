package com.datasphere.event;

import java.util.Map;

import com.datasphere.uuid.UUID;

public interface HDConvertible
{
    void convertFromDatallToRecord(final long p0, final UUID p1, final String p2, final Map<String, Object> p3);
}
