package org.jctools.queues;

import java.util.concurrent.atomic.*;

abstract class MpscCompoundQueueConsumerQueueIndex<E> extends MpscCompoundQueueMidPad<E>
{
    int consumerQueueIndex;
    protected volatile AtomicReference<Thread> emptyWaiter;
    
    public MpscCompoundQueueConsumerQueueIndex(final int capacity, final int queueParallelism) {
        super(capacity, queueParallelism);
        this.emptyWaiter = new AtomicReference<Thread>();
    }
}
