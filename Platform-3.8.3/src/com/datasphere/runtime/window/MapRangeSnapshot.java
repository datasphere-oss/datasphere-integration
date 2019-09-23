package com.datasphere.runtime.window;

import java.util.concurrent.*;

class MapRangeSnapshot extends SubMapSnapshot
{
    MapRangeSnapshot(final long vHead, final long vTail, final ConcurrentNavigableMap<Long, BufWindow.Bucket> els) {
        super(vHead, vTail, els);
    }
}
