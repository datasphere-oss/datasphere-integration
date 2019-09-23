package org.jctools.queues;

import java.util.concurrent.atomic.*;
import org.jctools.util.*;

abstract class MpscCompoundQueueColdFields<E> extends MpscCompoundQueueL0Pad<E>
{
    protected final int parallelQueues;
    protected final int parallelQueuesMask;
    protected final MpscArrayQueue<E>[] queues;
    protected final AtomicReference<Thread>[] fullWaiters;
    
    public MpscCompoundQueueColdFields(final int capacity, final int queueParallelism) {
        this.parallelQueues = (Pow2.isPowerOfTwo(queueParallelism) ? queueParallelism : (Pow2.roundToPowerOfTwo(queueParallelism) / 2));
        this.parallelQueuesMask = this.parallelQueues - 1;
        this.queues = (MpscArrayQueue<E>[])new MpscArrayQueue[this.parallelQueues];
        this.fullWaiters = (AtomicReference<Thread>[])new AtomicReference[this.parallelQueues];
        for (int i = 0; i < this.parallelQueues; ++i) {
            this.fullWaiters[i] = new AtomicReference<Thread>();
        }
        final int fullCapacity = Pow2.roundToPowerOfTwo(capacity);
        if (fullCapacity < this.parallelQueues) {
            throw new IllegalArgumentException("Queue capacity must exceed parallelism");
        }
        for (int j = 0; j < this.parallelQueues; ++j) {
            this.queues[j] = new MpscArrayQueue<E>(fullCapacity);
        }
    }
}
