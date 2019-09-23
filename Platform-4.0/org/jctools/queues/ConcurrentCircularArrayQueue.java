package org.jctools.queues;

import org.jctools.util.*;
import java.util.*;

public abstract class ConcurrentCircularArrayQueue<E> extends ConcurrentCircularArrayQueueL0Pad<E>
{
    protected final long mask;
    protected final E[] buffer;
    
    public ConcurrentCircularArrayQueue(final int capacity) {
        final int actualCapacity = Pow2.roundToPowerOfTwo(capacity);
        this.mask = actualCapacity - 1;
        this.buffer = SparsePaddedCircularArrayOffsetCalculator.allocate(actualCapacity);
    }
    
    protected final long calcElementOffset(final long index) {
        return calcElementOffset(index, this.mask);
    }
    
    protected static final long calcElementOffset(final long index, final long mask) {
        return SparsePaddedCircularArrayOffsetCalculator.calcElementOffset(index, mask);
    }
    
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void clear() {
        while (true) {
            if (this.poll() == null) {
                if (!this.isEmpty()) {
                    continue;
                }
                break;
            }
        }
    }
    
    @Override
    public int capacity() {
        return (int)(this.mask + 1L);
    }
}
