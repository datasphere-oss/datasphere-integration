package com.datasphere.runtime.window;

import java.util.concurrent.*;

class OverlappingSnapshot extends SubMapSnapshot
{
    final MapRangeSnapshot originalSnapshot;
    
    OverlappingSnapshot(final MapRangeSnapshot originalSnapshot, final long vTail, final ConcurrentSkipListMap<Long, BufWindow.Bucket> elements) {
        super(originalSnapshot.vHead, vTail, elements);
        assert originalSnapshot.vHead <= vTail;
        this.originalSnapshot = originalSnapshot;
    }
    
    public Snapshot getOriginalSnapshot() {
        return this.originalSnapshot;
    }
}
