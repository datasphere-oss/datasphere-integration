package org.jctools.queues.atomic;

import java.util.concurrent.atomic.*;

abstract class SequencedAtomicReferenceArrayQueue<E> extends AtomicReferenceArrayQueue<E>
{
    protected final AtomicLongArray sequenceBuffer;
    
    public SequencedAtomicReferenceArrayQueue(final int capacity) {
        super(capacity);
        final int actualCapacity = this.mask + 1;
        this.sequenceBuffer = new AtomicLongArray(actualCapacity);
        for (int i = 0; i < actualCapacity; ++i) {
            this.soSequence(this.sequenceBuffer, i, i);
        }
    }
    
    protected final long calcSequenceOffset(final long index) {
        return calcSequenceOffset(index, this.mask);
    }
    
    protected static final int calcSequenceOffset(final long index, final int mask) {
        return (int)index & mask;
    }
    
    protected final void soSequence(final AtomicLongArray buffer, final int offset, final long e) {
        buffer.lazySet(offset, e);
    }
    
    protected final long lvSequence(final AtomicLongArray buffer, final int offset) {
        return buffer.get(offset);
    }
}
