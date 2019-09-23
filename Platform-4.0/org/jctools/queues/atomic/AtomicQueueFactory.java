package org.jctools.queues.atomic;

import org.jctools.queues.spec.*;
import java.util.*;
import java.util.concurrent.*;

public class AtomicQueueFactory
{
    public static <E> Queue<E> newQueue(final ConcurrentQueueSpec qs) {
        if (qs.isBounded()) {
            if (qs.isSpsc()) {
                return new SpscAtomicArrayQueue<E>(qs.capacity);
            }
            if (qs.isMpsc()) {
                return new MpscAtomicArrayQueue<E>(qs.capacity);
            }
            if (qs.isSpmc()) {
                return new SpmcAtomicArrayQueue<E>(qs.capacity);
            }
            return new MpmcAtomicArrayQueue<E>(qs.capacity);
        }
        else {
            if (qs.isSpsc()) {
                return new SpscLinkedAtomicQueue<E>();
            }
            if (qs.isMpsc()) {
                return new MpscLinkedAtomicQueue<E>();
            }
            return new ConcurrentLinkedQueue<E>();
        }
    }
}
