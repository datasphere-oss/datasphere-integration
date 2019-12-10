package com.datasphere.distribution;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.containers.*;
import com.datasphere.proc.events.*;
import java.util.*;
import com.datasphere.runtime.components.*;
import java.util.concurrent.*;
import com.datasphere.runtime.*;
import com.datasphere.tungsten.*;

public class HQueue
{
    private static Logger logger;
    private static volatile WAQueueManager instance;
    private final String name;
    public final CopyOnWriteArrayList<Listener> listeners;
    public volatile boolean isActive;
    private BlockingQueue<Object> blockingQueue;
    private volatile boolean dequerReady;
    private Dequer dequer;
    private final int timeToPoll = 100;
    
    private static WAQueueManager getWAQueueManager() {
        if (HQueue.instance == null) {
            synchronized (WAQueueManager.class) {
                if (HQueue.instance == null) {
                    HQueue.instance = new WAQueueManager();
                }
            }
        }
        return HQueue.instance;
    }
    
    public String getName() {
        return this.name;
    }
    
    public static HQueue getQueue(final String queueName) {
        return getWAQueueManager().getQueue(queueName);
    }
    
    public static void removeQueue(final String queueName) {
        getWAQueueManager().removeQueue(queueName);
    }
    
    private HQueue(final String name) {
        this.listeners = new CopyOnWriteArrayList<Listener>();
        this.blockingQueue = new LinkedBlockingQueue<Object>(50000);
        this.dequerReady = false;
        this.dequer = null;
        this.name = name;
    }
    
    public void subscribeIfNotSubscribed(final Listener listener) throws Exception {
        if (!this.listeners.contains(listener)) {
            this.subscribe(listener);
        }
    }
    
    public void subscribe(final Listener listener) throws Exception {
        this.listeners.add(listener);
        getWAQueueManager().subscribeForStream();
        if (HQueue.logger.isDebugEnabled()) {
            HQueue.logger.debug((Object)("New Listener added for: " + this.name + ", total listeners: " + this.listeners.size()));
            HQueue.logger.debug((Object)"-------------------------");
        }
    }
    
    public void subscribeForTungsten(final Listener listener) throws Exception {
        this.listeners.add(listener);
        getWAQueueManager().subscribeForTungsten();
        if (HQueue.logger.isDebugEnabled()) {
            HQueue.logger.debug((Object)"Tungsten listener added");
        }
    }
    
    private void stopDequer() {
        this.dequerReady = false;
    }
    
    public void unsubscribe(final Listener listener) {
        this.listeners.remove(listener);
        this.stopDequer();
        this.dequer = null;
        this.blockingQueue.clear();
        if (this.listeners.size() == 0) {
            this.isActive = false;
            removeQueue(this.name);
        }
        getWAQueueManager().unsubscribe();
        if (HQueue.logger.isDebugEnabled()) {
            HQueue.logger.debug((Object)("Removed Listener on: " + this.name + ", total listeners: " + this.listeners.size()));
        }
    }
    
    public void put(final Object item) {
        getWAQueueManager().sendMessage(this.name, item);
    }
    
    @Override
    public String toString() {
        return "WAQueue(" + this.name + " has " + this.listeners.size() + " listeners)";
    }
    
    private void startDequer() {
        this.dequer = new Dequer(this.blockingQueue);
        this.dequerReady = true;
        this.dequer.start();
        Thread.yield();
    }
    
    private void putInQueue(final Object object) throws InterruptedException {
        this.blockingQueue.put(object);
    }
    
    static {
        HQueue.logger = Logger.getLogger((Class)HQueue.class);
        HQueue.instance = null;
    }
    
    public static class ShowStreamSubscriber implements Subscriber
    {
        public final UUID uuid;
        public final String name;
        
        public ShowStreamSubscriber() {
            this.uuid = new UUID(System.currentTimeMillis());
            this.name = "ShowStreamSubscriber-" + this.uuid.toString();
        }
        
        @Override
        public void receive(final Object linkID, final ITaskEvent event) throws Exception {
            for (final DARecord hdEvent : event.batch()) {
                final HQueueEvent queueEvent = (HQueueEvent)hdEvent.data;
                final String qKey = queueEvent.getKey();
                final HQueue waQueue = (HQueue)getWAQueueManager().instances.get(qKey);
                if (waQueue != null) {
                    synchronized (waQueue) {
                        if (!waQueue.dequerReady) {
                            waQueue.startDequer();
                        }
                        waQueue.putInQueue(queueEvent.getPayload());
                    }
                }
            }
        }
    }
    
    private class Dequer extends Thread
    {
        private final BlockingQueue<Object> blockingQueue;
        
        public Dequer(final BlockingQueue<Object> blockingQueue) {
            super(HQueue.this.name);
            this.blockingQueue = blockingQueue;
        }
        
        @Override
        public void run() {
            while (HQueue.this.dequerReady) {
                try {
                    final Object data = this.blockingQueue.poll(100L, TimeUnit.MILLISECONDS);
                    for (final Listener listener : HQueue.this.listeners) {
                        if (data != null) {
                            listener.onItem(data);
                        }
                    }
                }
                catch (InterruptedException e) {
                    HQueue.logger.error((Object)e.getMessage());
                }
            }
        }
    }
    
    private static class WAQueueManager
    {
        private volatile ConcurrentMap<String, HQueue> instances;
        private Link streamSubscriber;
        private Stream showStream;
        
        WAQueueManager() {
            this.instances = new ConcurrentHashMap<String, HQueue>();
            this.streamSubscriber = null;
            this.showStream = null;
            if (Server.server != null) {
                this.showStream = Server.server.getShowStream();
            }
            else if (HQueue.logger.isDebugEnabled()) {
                HQueue.logger.debug((Object)"Server is NULL");
            }
        }
        
        public void sendToClient(final String key, final Object o) {
            final HQueueEvent queueEvent = new HQueueEvent(key, o);
            if (this.showStream != null) {
                this.publish(queueEvent);
            }
            else if (Server.server != null) {
                this.showStream = Server.server.getShowStream();
                if (this.showStream != null) {
                    this.publish(queueEvent);
                }
                else if (HQueue.logger.isDebugEnabled()) {
                    HQueue.logger.debug((Object)"showStream is NULL, which is unexpected at this point)");
                }
            }
            else if (HQueue.logger.isDebugEnabled()) {
                HQueue.logger.debug((Object)"Server is NULLshowStream is NULL, which is unexpected at this point)");
            }
        }
        
        void sendMessage(final String key, final Object o) {
            this.sendToClient(key, o);
        }
        
        public void publish(final HQueueEvent queueEvent) {
            try {
                this.showStream.getChannel().publish(StreamEventFactory.createStreamEvent(queueEvent));
            }
            catch (Exception e) {
                HQueue.logger.error((Object)e.getMessage());
            }
        }
        
        HQueue getQueue(final String queueName) {
            HQueue queue = this.instances.get(queueName);
            if (queue == null) {
                final HQueue newqueue = new HQueue(queueName);
                queue = this.instances.putIfAbsent(queueName, newqueue);
                if (queue == null) {
                    queue = newqueue;
                }
            }
            if (HQueue.logger.isDebugEnabled()) {
                HQueue.logger.debug((Object)("Get WAQueue:" + queueName));
            }
            return queue;
        }
        
        void removeQueue(final String queueName) {
            final HQueue queue = this.instances.remove(queueName);
            if (queue != null) {
                queue.stopDequer();
                if (HQueue.logger.isDebugEnabled()) {
                    HQueue.logger.debug((Object)("Removed queue:" + queueName));
                }
            }
        }
        
        public void subscribeForStream() throws Exception {
            if (this.showStream == null) {
                if (Server.server != null) {
                    this.showStream = Server.server.getShowStream();
                }
                else if (HQueue.logger.isDebugEnabled()) {
                    HQueue.logger.debug((Object)"Server is NULL");
                }
            }
            if (this.showStream != null) {
                if (this.streamSubscriber == null) {
                    this.streamSubscriber = new Link(new ShowStreamSubscriber());
                    this.showStream.getChannel().addSubscriber(this.streamSubscriber);
                }
                if (HQueue.logger.isDebugEnabled()) {
                    HQueue.logger.debug((Object)"New subscriber added");
                }
            }
        }
        
        public void subscribeForTungsten() throws Exception {
            if (this.showStream == null && Tungsten.showStream != null) {
                this.showStream = Tungsten.showStream;
            }
            if (this.showStream != null && this.streamSubscriber == null) {
                this.streamSubscriber = new Link(new ShowStreamSubscriber());
                this.showStream.getChannel().addSubscriber(this.streamSubscriber);
                if (HQueue.logger.isDebugEnabled()) {
                    HQueue.logger.debug((Object)"New subscriber added");
                }
            }
        }
        
        public void unsubscribe() {
            if (this.instances.size() == 0) {
                this.showStream.getChannel().removeSubscriber(this.streamSubscriber);
                this.streamSubscriber = null;
                this.showStream = null;
            }
            if (HQueue.logger.isDebugEnabled()) {
                HQueue.logger.debug((Object)"Removed Stream Subscriber");
            }
        }
    }
    
    public interface Listener
    {
        void onItem(final Object p0);
    }
}
