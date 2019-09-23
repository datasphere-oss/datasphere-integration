package com.datasphere.messaging;

import java.util.concurrent.*;
import org.jctools.queues.*;
import com.datasphere.runtime.*;

public final class ArrayBQ<T> implements InterThreadComm
{
    private final BlockingQueue<T> queue;
    private final MSGSender eventPublisher;
    private final ChannelEventHandler channelEventHandler;
    final int sendQueueId;
    Status status;
    
    public ArrayBQ(final int size, final int sendQueueId, final ChannelEventHandler channelEventHandler) {
        this.queue = new MpscCompoundQueue<T>(size);
        this.sendQueueId = sendQueueId;
        this.channelEventHandler = channelEventHandler;
        (this.eventPublisher = new MSGSender(this.queue)).start();
        this.status = Status.RUNNING;
    }
    
    @Override
    public void put(final Object event, final DistSub distSub, final int partitionId, final ChannelEventHandler channelEventHandler) throws InterruptedException {
        if (this.status == Status.RUNNING) {
            final EventContainer container = new EventContainer();
            container.setAllFields(event, distSub, partitionId, channelEventHandler);
            this.queue.put((T)container);
        }
    }
    
    @Override
    public long size() {
        return 0L;
    }
    
    @Override
    public void stop() {
        this.status = Status.STOPPED;
        this.eventPublisher.interrupt();
        try {
            this.eventPublisher.join(10L);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public enum Status
    {
        UNKNOWN, 
        RUNNING, 
        STOPPED;
    }
    
    public class MSGSender extends Thread
    {
        private final BlockingQueue<EventContainer> eventQueue;
        
        MSGSender(final BlockingQueue eventQueue) {
            this.eventQueue = (BlockingQueue<EventContainer>)eventQueue;
        }
        
        @Override
        public void run() {
            try {
                while (!this.isInterrupted()) {
                    final EventContainer container = this.eventQueue.take();
                    if (container == null) {
                        continue;
                    }
                    ArrayBQ.this.channelEventHandler.sendEvent(container, ArrayBQ.this.sendQueueId, null);
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
