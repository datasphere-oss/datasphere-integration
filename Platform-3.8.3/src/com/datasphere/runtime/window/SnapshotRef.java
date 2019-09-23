package com.datasphere.runtime.window;

import java.lang.ref.*;

public abstract class SnapshotRef extends WeakReference<Snapshot>
{
    final BufWindow buffer;
    
    SnapshotRef(final BufWindow buffer, final Snapshot referent, final ReferenceQueue<? super Snapshot> q) {
        super(referent, q);
        this.buffer = buffer;
    }
    
    @Override
    public abstract String toString();
}
