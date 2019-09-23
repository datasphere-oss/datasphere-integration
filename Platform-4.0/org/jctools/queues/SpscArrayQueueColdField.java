package org.jctools.queues;

abstract class SpscArrayQueueColdField<E> extends ConcurrentCircularArrayQueue<E>
{
    static final int MAX_LOOK_AHEAD_STEP;
    protected final int lookAheadStep;
    
    public SpscArrayQueueColdField(final int capacity) {
        super(capacity);
        this.lookAheadStep = Math.min(capacity / 4, SpscArrayQueueColdField.MAX_LOOK_AHEAD_STEP);
    }
    
    static {
        MAX_LOOK_AHEAD_STEP = Integer.getInteger("jctools.spsc.max.lookahead.step", 4096);
    }
}
