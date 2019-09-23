package org.jctools.queues.spec;

public final class ConcurrentQueueSpec
{
    public final int producers;
    public final int consumers;
    public final int capacity;
    public final Ordering ordering;
    public final Preference preference;
    
    public static ConcurrentQueueSpec createBoundedSpsc(final int capacity) {
        return new ConcurrentQueueSpec(1, 1, capacity, Ordering.FIFO, Preference.NONE);
    }
    
    public static ConcurrentQueueSpec createBoundedMpsc(final int capacity) {
        return new ConcurrentQueueSpec(0, 1, capacity, Ordering.FIFO, Preference.NONE);
    }
    
    public static ConcurrentQueueSpec createBoundedSpmc(final int capacity) {
        return new ConcurrentQueueSpec(1, 0, capacity, Ordering.FIFO, Preference.NONE);
    }
    
    public static ConcurrentQueueSpec createBoundedMpmc(final int capacity) {
        return new ConcurrentQueueSpec(0, 0, capacity, Ordering.FIFO, Preference.NONE);
    }
    
    public ConcurrentQueueSpec(final int producers, final int consumers, final int capacity, final Ordering ordering, final Preference preference) {
        this.producers = producers;
        this.consumers = consumers;
        this.capacity = capacity;
        this.ordering = ordering;
        this.preference = preference;
    }
    
    public boolean isSpsc() {
        return this.consumers == 1 && this.producers == 1;
    }
    
    public boolean isMpsc() {
        return this.consumers == 1 && this.producers != 1;
    }
    
    public boolean isSpmc() {
        return this.consumers != 1 && this.producers == 1;
    }
    
    public boolean isMpmc() {
        return this.consumers != 1 && this.producers != 1;
    }
    
    public boolean isBounded() {
        return this.capacity != 0;
    }
}
