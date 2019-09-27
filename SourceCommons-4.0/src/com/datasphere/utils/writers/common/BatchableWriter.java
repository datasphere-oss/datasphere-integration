package com.datasphere.utils.writers.common;

import java.util.*;

import com.datasphere.recovery.*;

public interface BatchableWriter
{
    List<Position> write(final String p0, final List<EventDataObject> p1) throws Exception;
}
