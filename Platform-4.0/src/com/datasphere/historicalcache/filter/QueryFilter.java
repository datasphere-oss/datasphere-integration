package com.datasphere.historicalcache.filter;

import java.util.*;

import com.datasphere.cache.*;
import com.datasphere.runtime.containers.*;

public class QueryFilter implements Filter<Object, Map<Long, List<DARecord>>>
{
    long snapshotId;
    
    public QueryFilter(final long snapshotId) {
        this.snapshotId = snapshotId;
    }
    
    @Override
    public boolean matches(final Object key, final Map<Long, List<DARecord>> value) {
        return value.containsKey(this.snapshotId);
    }
}
