package org.jctools.queues;

import java.util.*;
import org.jctools.queues.spec.*;
import java.util.concurrent.*;

public class QueueFactory
{
    public static <E> Queue<E> newQueue(final ConcurrentQueueSpec qs) {
        if (qs.isBounded()) {
            if (qs.isSpsc()) {
                return new SpscArrayQueue<E>(qs.capacity);
            }
            if (qs.isMpsc()) {
                if (qs.ordering != Ordering.NONE) {
                    return new MpscArrayQueue<E>(qs.capacity);
                }
                return new MpscCompoundQueue<E>(qs.capacity);
            }
            else {
                if (qs.isSpmc()) {
                    return new SpmcArrayQueue<E>(qs.capacity);
                }
                return new MpmcArrayQueue<E>(qs.capacity);
            }
        }
        else {
            if (qs.isSpsc()) {
                return new SpscLinkedQueue<E>();
            }
            if (qs.isMpsc()) {
                return new MpscLinkedQueue7<E>();
            }
            return new ConcurrentLinkedQueue<E>();
        }
    }
}
