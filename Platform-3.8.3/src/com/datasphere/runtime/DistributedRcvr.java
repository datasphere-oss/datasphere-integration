package com.datasphere.runtime;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.datasphere.messaging.Handler;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.UnknownObjectException;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.proc.records.PublishableRecord;
import com.datasphere.proc.records.Record;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.uuid.UUID;
import com.hazelcast.core.IQueue;

public class DistributedRcvr implements Handler
{
    private static Logger logger;
    public Pair<DistLink, Link> subscriber;
    protected String receiverName;
    private DistributedChannel channel;
    private boolean debugEventLoss;
    private boolean debugEventCountMatch;
    public Status status;
    public FlowComponent owner;
    long countt;
    long lastCount;
    long delta;
    long stime;
    long etime;
    long ttime;
    long tdiff;
    final MessagingSystem messagingSystem;
    Map<UUID, Integer> peerCounts;
    
    public DistributedRcvr(final String receiverName, final FlowComponent owner, final DistributedChannel pStream, final boolean encrypted, final MessagingSystem messagingSystem) {
        this.debugEventLoss = false;
        this.debugEventCountMatch = false;
        this.countt = -1L;
        this.stime = 0L;
        this.etime = 0L;
        this.ttime = 0L;
        this.peerCounts = new ConcurrentHashMap<UUID, Integer>();
        if (DistributedRcvr.logger.isInfoEnabled()) {
            DistributedRcvr.logger.info((Object)(this.getClass().getSimpleName() + " for " + receiverName + " created."));
        }
        (this.messagingSystem = messagingSystem).createReceiver(messagingSystem.getReceiverClass(), this, receiverName, encrypted, pStream.getOwner().getMetaInfo());
        this.owner = owner;
        this.channel = pStream;
        this.status = Status.INITIALIZED;
        this.receiverName = receiverName;
    }
    
    public void startReceiver(final String name, final Map<Object, Object> properties) throws Exception {
        if (this.status == Status.INITIALIZED) {
            this.messagingSystem.startReceiver(name, properties);
            this.start();
        }
    }
    
    public static float ns2s(final long nsecs) {
        final float f = nsecs;
        final float t = 1.0E9f;
        return f / t;
    }
    
    public void stats2() {
        if (DistributedRcvr.logger.isDebugEnabled()) {
            DistributedRcvr.logger.debug((Object)("Stream Stats @" + this.owner.getMetaName() + " : \nTotal messages : " + this.countt + "\nTotal time :  " + ns2s(this.ttime) + " seconds , \nRate(nrOfItems/sec) : " + this.delta * 1000000000L / this.tdiff + "\n-----------------------------------------------------------------"));
        }
    }
    
    @Override
    public void onMessage(final Object data) {
        if (data instanceof StreamEvent) {
            final StreamEvent event = (StreamEvent)data;
            if (event.getLink() == null) {
                this.doReceive(event.getTaskEvent());
            }
            else {
                this.doReceive(event.getLink(), event.getTaskEvent());
            }
        }
        else if (data instanceof PublishableRecord) {
            final PublishableRecord pubEvent = (PublishableRecord)data;
            final StreamEvent event2 = pubEvent.getStreamEvent();
            this.doReceive(event2.getLink(), event2.getTaskEvent());
        }
        else if (data != null) {
            throw new UnknownObjectException("Expecting Stream Event at " + this.owner.getMetaName() + ", got : " + data.getClass());
        }
        if (DistributedRcvr.logger.isDebugEnabled()) {
            ++this.countt;
            if (this.countt == 0L) {
                this.stime = System.nanoTime();
            }
            if (this.countt % 500000L == 0L && this.countt != 0L) {
                this.etime = System.nanoTime();
                this.tdiff = this.etime - this.stime;
                this.ttime += this.tdiff;
                this.stime = this.etime;
                this.delta = this.countt - this.lastCount;
                this.lastCount = this.countt;
                this.stats2();
            }
        }
    }
    
    @Override
    public String getName() {
        return this.receiverName;
    }
    
    @Override
    public FlowComponent getOwner() {
        return this.owner;
    }
    
    public void stop() {
        if (this.debugEventCountMatch) {
            DistributedRcvr.logger.warn((Object)(this.owner.getMetaName() + " Events received " + this.peerCounts));
        }
        this.status = Status.INITIALIZED;
    }
    
    public void start() {
        this.status = Status.RUNNING;
    }
    
    public void close() {
        try {
            final MessagingSystem messagingSystem = this.channel.messagingSystem;
            messagingSystem.stopReceiver(this.receiverName);
            this.owner = null;
            this.channel = null;
            this.receiverName = null;
        }
        catch (Exception e) {
            DistributedRcvr.logger.error((Object)("Unable to Stop Stream Receiver: " + this.receiverName), (Throwable)e);
        }
    }
    
    public void addSubscriber(final Link link, final DistLink key) {
        this.subscriber = new Pair<DistLink, Link>(key, link);
        if (DistributedRcvr.logger.isDebugEnabled()) {
            this.showSubscriptions();
        }
    }
    
    public void showSubscriptions() {
        if (DistributedRcvr.logger.isDebugEnabled()) {
            DistributedRcvr.logger.debug((Object)("--> Local Subscriptions for " + this.channel.getMetaObject().getName() + " - " + this.channel.getMetaObject().getUuid()));
            DistributedRcvr.logger.debug((Object)("    -- " + this.subscriber.second));
        }
    }
    
    private void doReceive(final ITaskEvent taskEvent) {
        this.doReceive(this.subscriber.first, taskEvent);
    }
    
    public void doReceive(final DistLink link, final ITaskEvent event) {
        final Link l = this.subscriber.second;
        if (l != null) {
            try {
                if (event instanceof CommandEvent) {
                    final CommandEvent commandEvent = (CommandEvent)event;
                    try {
                        if (!commandEvent.performCommandForStream(this.getOwner(), l.subscriber.hashCode())) {
                            return;
                        }
                        DistributedRcvr.logger.info((Object)("Sending command event to subscriber " + l.subscriber.getClass().getName()));
                    }
                    catch (Exception e) {
                        DistributedRcvr.logger.error((Object)e.getMessage(), (Throwable)e);
                    }
                }
                l.subscriber.receive(l.linkID, event);
                this.debugEvents(link, event);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            catch (Exception e2) {
                DistributedRcvr.logger.error((Object)"Error receiving event", (Throwable)e2);
            }
        }
        else if (DistributedRcvr.logger.isDebugEnabled()) {
            DistributedRcvr.logger.debug((Object)(this.channel.getDebugId() + " tried to pass event to unknown subscriber " + link));
        }
    }
    
    private void debugEvents(final DistLink link, final ITaskEvent event) {
        if (this.debugEventCountMatch) {
            Integer count = this.peerCounts.get(link.getSubID());
            if (count == null) {
                count = 0;
            }
            this.peerCounts.put(link.getSubID(), count + 1);
        }
        if (this.debugEventLoss) {
            final IQueue<String> evTS = HazelcastSingleton.get().getQueue(this.channel.getMetaObject().getUuid() + "-" + link.getSubID() + "-evTS");
            for (final DARecord Record : event.batch()) {
                String next = null;
                for (int count2 = 0; count2 < 10; ++count2) {
                    next = this.waitForEvent(evTS, next);
                    next = this.matchEvent(evTS, Record, next);
                    if (next != null) {
                        break;
                    }
                }
                if (next == null) {
                    DistributedRcvr.logger.warn((Object)("Stream: " + this.owner.getMetaName() + "Did not find a matching event at all"));
                }
            }
        }
    }
    
    private String matchEvent(final IQueue<String> evTS, final DARecord Record, String next) {
        while (next != null && !next.equals(Record.toString())) {
            DistributedRcvr.logger.warn((Object)("Stream: " + this.owner.getMetaName() + "Missing event " + next));
            try {
                next = (String)evTS.remove();
                continue;
            }
            catch (NoSuchElementException e) {
                return null;
            }
        }
        return next;
    }
    
    private String waitForEvent(final IQueue<String> evTS, String next) {
        while (next == null) {
            try {
                next = (String)evTS.remove();
            }
            catch (Exception ex) {}
        }
        return next;
    }
    
    static {
        DistributedRcvr.logger = Logger.getLogger((Class)DistributedRcvr.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        RUNNING;
    }
}
