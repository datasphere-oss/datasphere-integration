package com.datasphere.messaging;

import com.lmax.disruptor.dsl.*;
import java.util.concurrent.*;
import com.lmax.disruptor.*;
import com.datasphere.runtime.*;

public class HDisruptor<T> implements InterThreadComm
{
    private Disruptor disruptor;
    private final EventPublisherWithTranslator eventPublisher;
    WaitStrategy waitStrategy;
    ExecutorService executorService;
    Status status;
    
    public HDisruptor(final int size, final int sendQueueId, final ChannelEventHandler channelEventHandler) {
        this(size, sendQueueId, channelEventHandler, WAITSTRATEGY.yield.name(), "Disruptor");
    }
    
    public HDisruptor(final int size, final int sendQueueId, final ChannelEventHandler channelEventHandler, final String waitstrategy, final String name) {
        this.executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = new Thread(r);
                t.setName(name + "-" + System.currentTimeMillis());
                return t;
            }
        });
        this.waitStrategy = this.getStrategy(waitstrategy);
        (this.disruptor = new Disruptor((EventFactory)new StreamEventFactory(), size, (Executor)this.executorService, ProducerType.SINGLE, this.waitStrategy)).handleEventsWith(new EventHandler[] { new EventHandler<T>() {
                public void onEvent(final T event, final long sequence, final boolean endOfBatch) throws Exception {
                    if (event instanceof EventContainer) {
                        final EventContainer container = (EventContainer)event;
                        channelEventHandler.sendEvent(container, sendQueueId, null);
                    }
                }
            } });
        this.disruptor.start();
        this.eventPublisher = new EventPublisherWithTranslator((RingBuffer<EventContainer>)this.disruptor.getRingBuffer());
        this.status = Status.RUNNING;
    }
    
    @Override
    public void put(final Object waEvent, final DistSub s, final int partitionId, final ChannelEventHandler channelEventHandler) {
        if (this.status == Status.RUNNING) {
            this.eventPublisher.onData(waEvent, s, partitionId, channelEventHandler);
        }
    }
    
    @Override
    public long size() {
        return this.eventPublisher.size();
    }
    
    @Override
    public void stop() {
        this.status = Status.STOPPED;
        this.disruptor.halt();
        this.disruptor.shutdown();
        this.executorService.shutdownNow();
    }
    
    public WaitStrategy getStrategy() {
        final String strategy = System.getProperty("com.datasphere.config.waitStrategy");
        WAITSTRATEGY waitstrategy;
        if (strategy != null) {
            try {
                waitstrategy = WAITSTRATEGY.valueOf(strategy);
            }
            catch (IllegalArgumentException e) {
                waitstrategy = WAITSTRATEGY.yield;
            }
        }
        else {
            waitstrategy = WAITSTRATEGY.yield;
        }
        switch (waitstrategy) {
            case busyspin: {
                return (WaitStrategy)new BusySpinWaitStrategy();
            }
            case yield: {
                return (WaitStrategy)new YieldingWaitStrategy();
            }
            case sleep: {
                return (WaitStrategy)new SleepingWaitStrategy();
            }
            case block: {
                return (WaitStrategy)new BlockingWaitStrategy();
            }
            default: {
                return (WaitStrategy)new YieldingWaitStrategy();
            }
        }
    }
    
    public WaitStrategy getStrategy(final String strategy) {
        WAITSTRATEGY waitstrategy;
        if (strategy != null) {
            try {
                waitstrategy = WAITSTRATEGY.valueOf(strategy);
            }
            catch (IllegalArgumentException e) {
                waitstrategy = WAITSTRATEGY.yield;
            }
        }
        else {
            waitstrategy = WAITSTRATEGY.yield;
        }
        switch (waitstrategy) {
            case busyspin: {
                return (WaitStrategy)new BusySpinWaitStrategy();
            }
            case yield: {
                return (WaitStrategy)new YieldingWaitStrategy();
            }
            case sleep: {
                return (WaitStrategy)new SleepingWaitStrategy();
            }
            case block: {
                return (WaitStrategy)new BlockingWaitStrategy();
            }
            default: {
                return (WaitStrategy)new YieldingWaitStrategy();
            }
        }
    }
    
    public enum Status
    {
        UNKNOWN, 
        RUNNING, 
        STOPPED;
    }
    
    public enum WAITSTRATEGY
    {
        busyspin, 
        yield, 
        sleep, 
        block;
    }
    
    public class EventPublisherWithTranslator
    {
        private final RingBuffer<EventContainer> ringBuffer;
        private final EventTranslatorVararg<EventContainer> TRANSLATOR2;
        
        public EventPublisherWithTranslator(final RingBuffer<EventContainer> ringBuffer) {
            this.TRANSLATOR2 = (EventTranslatorVararg<EventContainer>)new EventTranslatorVararg<EventContainer>() {
                public void translateTo(final EventContainer event, final long sequence, final Object... args) {
                    event.setAllFields(args[0], (DistSub)args[1], (int)args[2], (ChannelEventHandler)args[3]);
                }
            };
            this.ringBuffer = ringBuffer;
        }
        
        public void onData(final Object event, final DistSub s, final int partitionId, final ChannelEventHandler channelEventHandler) {
            this.ringBuffer.publishEvent((EventTranslatorVararg)this.TRANSLATOR2, new Object[] { event, s, partitionId, channelEventHandler });
        }
        
        public long size() {
            return this.ringBuffer.remainingCapacity();
        }
    }
}
