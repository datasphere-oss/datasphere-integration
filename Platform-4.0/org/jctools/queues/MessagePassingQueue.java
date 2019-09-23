package org.jctools.queues;

public interface MessagePassingQueue<T>
{
    public static final int UNBOUNDED_CAPACITY = -1;
    
    boolean offer(final T p0);
    
    T poll();
    
    T peek();
    
    int size();
    
    void clear();
    
    boolean isEmpty();
    
    int capacity();
    
    boolean relaxedOffer(final T p0);
    
    T relaxedPoll();
    
    T relaxedPeek();
    
    int drain(final Consumer<T> p0);
    
    int fill(final Supplier<T> p0);
    
    int drain(final Consumer<T> p0, final int p1);
    
    int fill(final Supplier<T> p0, final int p1);
    
    void drain(final Consumer<T> p0, final WaitStrategy p1, final ExitCondition p2);
    
    void fill(final Supplier<T> p0, final WaitStrategy p1, final ExitCondition p2);
    
    public interface ExitCondition
    {
        boolean keepRunning();
    }
    
    public interface WaitStrategy
    {
        int idle(final int p0);
    }
    
    public interface Consumer<T>
    {
        void accept(final T p0);
    }
    
    public interface Supplier<T>
    {
        T get();
    }
}
