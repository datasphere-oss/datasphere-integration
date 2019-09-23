package com.datasphere.runtime.window;

import java.lang.ref.*;
import java.util.*;

public interface SnapshotRefIndex
{
    void add(final BufWindow p0, final Snapshot p1, final ReferenceQueue<? super Snapshot> p2);
    
    long removeAndGetOldest(final SnapshotRef p0);
    
    List<? extends SnapshotRef> toList();
    
    boolean isEmpty();
    
    int size();
}
