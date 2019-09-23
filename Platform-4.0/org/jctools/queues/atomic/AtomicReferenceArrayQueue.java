package org.jctools.queues.atomic;

import java.util.concurrent.atomic.*;
import org.jctools.util.*;
import java.util.*;

abstract class AtomicReferenceArrayQueue<E> extends AbstractQueue<E>
{
    protected final AtomicReferenceArray<E> buffer;
    protected final int mask;
    
    public AtomicReferenceArrayQueue(final int capacity) {
        final int actualCapacity = Pow2.roundToPowerOfTwo(capacity);
        this.mask = actualCapacity - 1;
        this.buffer = new AtomicReferenceArray<E>(actualCapacity);
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
    
    protected final int calcElementOffset(final long index, final int mask) {
        return (int)index & mask;
    }
    
    protected final int calcElementOffset(final long index) {
        return (int)index & this.mask;
    }
    
    protected final E lvElement(final AtomicReferenceArray<E> buffer, final int offset) {
        return buffer.get(offset);
    }
    
    protected final E lpElement(final AtomicReferenceArray<E> buffer, final int offset) {
        return buffer.get(offset);
    }
    
    protected final E lpElement(final int offset) {
        return this.buffer.get(offset);
    }
    
    protected final void spElement(final AtomicReferenceArray<E> buffer, final int offset, final E value) {
        buffer.lazySet(offset, value);
    }
    
    protected final void spElement(final int offset, final E value) {
        this.buffer.lazySet(offset, value);
    }
    
    protected final void soElement(final AtomicReferenceArray<E> buffer, final int offset, final E value) {
        buffer.lazySet(offset, value);
    }
    
    protected final void soElement(final int offset, final E value) {
        this.buffer.lazySet(offset, value);
    }
    
    protected final void svElement(final AtomicReferenceArray<E> buffer, final int offset, final E value) {
        buffer.set(offset, value);
    }
    
    protected final E lvElement(final int offset) {
        return this.lvElement(this.buffer, offset);
    }
}
