package com.datasphere.runtime.channels;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.datasphere.messaging.MessagingProvider;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepositoryUtils;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.DistSub;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.ManagedZMQDistSub;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.UnManagedZMQDistSub;
import com.datasphere.runtime.ZMQDistSub;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.uuid.UUID;

public class ZMQChannel extends DistributedChannel implements ItemListener<DistLink>
{
    private static Logger logger;
    private ISet<DistLink> subscribers;
    public final ConcurrentHashMap<DistLink, ZMQDistSub> subscriptions;
    private Channel.NewSubscriberAddedCallback callback;
    private String itemListenerId;
    private Map<UUID, Set<UUID>> thisisimportant;
    
    public ZMQChannel(final Stream owner) {
        super(owner);
        this.subscriptions = new ConcurrentHashMap<DistLink, ZMQDistSub>();
        if (owner == null) {
            throw new RuntimeException("DistributedKryoChannel has no owner");
        }
        if (this.isCurrentStreamEncrypted()) {
            this.encrypted = true;
        }
        this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
        this.subscribers = HazelcastSingleton.get().getSet("#sharedStreamSubs-" + this.getID());
        this.itemListenerId = this.subscribers.addItemListener((ItemListener)this, true);
        for (final DistLink distLink : this.subscribers) {
            if (!distLink.getName().equals("Global.ExceptionManager") && !distLink.getName().equals("MonitoringCQ") && ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Found subscriber: " + distLink.getName() + "for stream: " + owner.getMetaFullName()));
            }
        }
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)(this.getDebugId() + " distributed channel created"));
        }
    }
    
    public ZMQChannel(final BaseServer srvr, final Stream owner) {
        super(owner);
        this.subscriptions = new ConcurrentHashMap<DistLink, ZMQDistSub>();
        if (owner == null) {
            throw new RuntimeException("DistributedKryoChannel has no owner");
        }
        this.srv = srvr;
        if (this.isCurrentStreamEncrypted()) {
            this.encrypted = true;
        }
        this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
        this.subscribers = HazelcastSingleton.get().getSet("#sharedStreamSubs-" + this.getID());
        this.itemListenerId = this.subscribers.addItemListener((ItemListener)this, true);
        for (final DistLink distLink : this.subscribers) {
            if (!distLink.getName().equals("Global.ExceptionManager") && !distLink.getName().equals("MonitoringCQ") && ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Found subscriber: " + distLink.getName() + "for stream: " + owner.getMetaFullName()));
            }
            if (!(srvr instanceof Server)) {
                this.createGetDistSub(distLink, true);
            }
            else {
                this.createGetDistSub(distLink);
            }
        }
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)(this.getDebugId() + " distributed channel created"));
        }
    }
    
    @Override
    public Set<DistLink> getSubscribers() {
        return this.subscriptions.keySet();
    }
    
    @Override
    public void closeDistSub(final String uuid) {
        Map.Entry<DistLink, ZMQDistSub> current = null;
        for (final Map.Entry<DistLink, ZMQDistSub> entry : this.subscriptions.entrySet()) {
            if (entry.getValue().hasThisPeer(new UUID(uuid))) {
                current = entry;
                break;
            }
        }
        if (current != null) {
            current.getValue().stop();
            final boolean removed = this.subscribers.remove((Object)current.getKey());
            this.subscriptions.remove(current.getKey());
            if (removed) {
                if (ZMQChannel.logger.isInfoEnabled()) {
                    ZMQChannel.logger.info((Object)("Tungsten subscription to showStream removed, current count in Subscriber ISet: " + this.subscribers.size() + " and count is subscription local map: " + this.subscriptions.size()));
                }
                else if (ZMQChannel.logger.isInfoEnabled()) {
                    ZMQChannel.logger.info((Object)("Tunsgten subscription to showStream was not removed, current count in Subscriber ISet: " + this.subscribers.size() + " and count is subscription local map: " + this.subscriptions.size()));
                }
            }
        }
    }
    
    @Override
    public Stream getOwner() {
        return this.owner;
    }
    
    private boolean isCurrentStreamEncrypted() {
        final MetaInfo.Stream currentStreamInfo = this.getStreamInfo();
        final MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(currentStreamInfo);
        if (appInfo != null) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)(currentStreamInfo.name + " belongs to app:" + appInfo.name + ", encryption:" + appInfo.encrypted));
            }
            return appInfo.encrypted;
        }
        return false;
    }
    
    public void publish(final ITaskEvent event) throws Exception {
        if (this.status != Status.RUNNING) {
            return;
        }
        try {
            if (event instanceof CommandEvent) {
                for (final ZMQDistSub s : this.subscriptions.values()) {
                    s.sendCommand((CommandEvent)event, this.thisPeerId);
                }
            }
            else if (this.keyFactory != null) {
                final IBatch<DARecord> DARecordIBatch = (IBatch<DARecord>)event.batch();
                for (final DARecord DARecord : DARecordIBatch) {
                    final RecordKey key = this.keyFactory.makeKey(DARecord.data);
                    for (final ZMQDistSub s2 : this.subscriptions.values()) {
                        final LagMarker lagMarker = ((TaskEvent)event).getLagMarker();
                        LagMarker copy = null;
                        if (lagMarker != null) {
                            copy = lagMarker.copy();
                        }
                        s2.sendPartitioned(DARecord, key, copy);
                    }
                }
            }
            else {
                int sends = 0;
                for (final ZMQDistSub s3 : this.subscriptions.values()) {
                    s3.sendUnpartitioned(event, this.thisPeerId, ++sends);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }
    
    private void showSubscriptions() {
        if (ZMQChannel.logger.isDebugEnabled()) {
            ZMQChannel.logger.debug((Object)("--> Remote Subscriptions for " + this.getName() + " - " + this.getID()));
            for (final DistSub sub : this.subscriptions.values()) {
                ZMQChannel.logger.debug((Object)("    -- " + sub));
            }
        }
    }
    
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        return Collections.emptyList();
    }
    
    public void addCallback(final Channel.NewSubscriberAddedCallback callback) {
        this.callback = callback;
    }
    
    public int getSubscribersCount() {
        return this.subscriptions.size();
    }
    
    public void addSubscriber(final Link link) {
        if (ZMQChannel.logger.isDebugEnabled()) {
            ZMQChannel.logger.debug((Object)("--> " + this.getName() + " add subscriber " + link));
        }
        if (this.callback != null) {
            this.callback.notifyMe(link);
        }
        final UUID subID = this.getSubscriberID(link.subscriber);
        final String name = this.getSubscriberName(link.subscriber);
        final DistLink key = new DistLink(subID, link.linkID, name);
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)(this.getDebugId() + " add subscription from " + key));
        }
        final boolean isPresent = this.checkIfSubscriberAndStreamBelongToSameApp(subID);
        final DistSub dist_sub = this.createGetDistSub(key, isPresent);
        if (link.subscriber.canReceiveDataSynchronously()) {
            dist_sub.setReceiveDataSynchronously(true);
        }
        if (isPresent) {
            dist_sub.addPeer(this.thisPeerId);
        }
        else {
            dist_sub.addPeer(this.thisPeerId);
        }
        final String receiverName = DistributedChannel.buildReceiverName(this.stream_metainfo, subID);
        dist_sub.addDistributedRcvr(receiverName, this.owner, this, this.encrypted, this.messagingSystem, link, this.thisisimportant);
        if (this.status == Status.RUNNING) {
            if (this.thisisimportant == null) {
                dist_sub.start();
            }
            else {
                dist_sub.start(this.thisisimportant);
            }
            try {
                dist_sub.startDistributedReceiver(this.thisisimportant);
            }
            catch (Exception e) {
                ZMQChannel.logger.error((Object)e.getMessage(), (Throwable)e);
            }
        }
        this.subscribers.add(key);
    }
    
    private synchronized DistSub createGetDistSub(final DistLink key) {
        return this.createGetDistSub(key, false);
    }
    
    public void removeSubscriber(final Link link) {
        if (ZMQChannel.logger.isDebugEnabled()) {
            ZMQChannel.logger.debug((Object)("--> " + this.getName() + " remove subscriber " + link));
        }
        final UUID subID = this.getSubscriberID(link.subscriber);
        final String name = this.getSubscriberName(link.subscriber);
        final DistLink key = new DistLink(subID, link.linkID, name);
        if (key != null) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)(this.getDebugId() + " remove subscription from " + key));
            }
            final ZMQDistSub sub = this.subscriptions.remove(key);
            if (sub != null) {
                sub.close(this.thisPeerId);
            }
            this.subscribers.remove((Object)key);
        }
        this.showSubscriptions();
    }
    
    public void itemAdded(final ItemEvent<DistLink> distLinkItemEvent) {
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)("New susbscriber added to " + this.getName() + " on remote node(" + distLinkItemEvent.getMember().getUuid() + ") so adding on this node as well : (" + BaseServer.getServerName() + ", " + BaseServer.getBaseServer().getServerID() + ")"));
        }
        final DistLink key = (DistLink)distLinkItemEvent.getItem();
        final boolean isPresent = this.checkIfSubscriberAndStreamBelongToSameApp(key.getSubID());
        final DistSub dist_sub = this.createGetDistSub(key, isPresent);
        if (this.status == Status.RUNNING) {
            dist_sub.start(this.thisisimportant);
        }
        this.showSubscriptions();
    }
    
    public void itemRemoved(final ItemEvent<DistLink> distLinkItemEvent) {
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)("Removing susbscriber to " + this.getName() + " on : " + BaseServer.getServerName() + " because it was removed on remote node(" + distLinkItemEvent.getMember().getUuid() + ")"));
        }
        final DistLink key = (DistLink)distLinkItemEvent.getItem();
        this.showSubscriptions();
        final UUID peerId = this.thisPeerId;
        final ZMQDistSub sub = this.subscriptions.remove(key);
        if (sub != null) {
            sub.close(peerId);
        }
    }
    
    @Override
    public synchronized void stop() {
        if (this.status != Status.RUNNING) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Stream " + this.getName() + " is not running so nothing to stop"));
            }
            return;
        }
        for (final ZMQDistSub s : this.subscriptions.values()) {
            s.stop();
        }
        if (ZMQChannel.logger.isInfoEnabled()) {
            ZMQChannel.logger.info((Object)("Stream " + this.getName() + " stopped"));
        }
        this.status = Status.INITIALIZED;
    }
    
    @Override
    public synchronized void start() {
        if (this.status != Status.INITIALIZED) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("ZMQChannel " + this.getName() + " is already running so cannot start it again."));
            }
            return;
        }
        for (final DistSub s : this.subscriptions.values()) {
            s.start();
        }
        this.status = Status.RUNNING;
        if (this.subscriptions.isEmpty()) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Started ZMQChannel " + this.getName() + " without subscribers"));
            }
            else if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Started ZMQChannel " + this.getName() + " with " + this.subscriptions.size() + " subscribers"));
            }
        }
    }
    
    @Override
    public synchronized void start(final Map<UUID, Set<UUID>> servers) {
        if (this.status != Status.INITIALIZED) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("ZMQChannel " + this.getName() + " is already running so cannot start it again."));
            }
            return;
        }
        this.thisisimportant = servers;
        for (final DistSub s : this.subscriptions.values()) {
            s.start(servers);
        }
        this.status = Status.RUNNING;
        if (this.subscriptions.isEmpty()) {
            if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Started ZMQChannel " + this.getName() + " without subscribers"));
            }
            else if (ZMQChannel.logger.isInfoEnabled()) {
                ZMQChannel.logger.info((Object)("Started ZMQChannel " + this.getName() + " with " + this.subscriptions.size() + " subscribers"));
            }
        }
    }
    
    @Override
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        if (this.status != Status.RUNNING) {
            return false;
        }
        if (this.subscriptions.size() < this.subscribers.size()) {
            return false;
        }
        for (final Map.Entry<DistLink, ZMQDistSub> s : this.subscriptions.entrySet()) {
            if (!s.getValue().verifyStart(serverToDeployedObjects)) {
                return false;
            }
        }
        return true;
    }
    
    public void close() throws IOException {
        this.stop();
        this.subscribers.removeItemListener(this.itemListenerId);
        this.itemListenerId = null;
        if (ZMQChannel.logger.isInfoEnabled()) {
            try {
                ZMQChannel.logger.info((Object)(this.getDebugId() + " close " + this.getName() + " on " + DistributedChannel.id2name(this.thisPeerId)));
            }
            catch (MetaDataRepositoryException e) {
                ZMQChannel.logger.error((Object)("Could not print message? " + e.getMessage()), (Throwable)e);
            }
        }
        this.showSubscriptions();
        try {
            KeyFactory.removeKeyFactory(this.getStreamInfo(), this.srv);
        }
        catch (Exception e2) {
            ZMQChannel.logger.error((Object)"Error while removing key factory", (Throwable)e2);
        }
        this.srv = null;
    }
    
    @Override
    public boolean isFull() {
        for (final ZMQDistSub s : this.subscriptions.values()) {
            if (s.isFull()) {
                return true;
            }
        }
        return false;
    }
    
    private synchronized DistSub createGetDistSub(final DistLink key, final boolean isManaged) {
        ZMQDistSub s = this.subscriptions.get(key);
        if (s == null) {
            if (isManaged) {
                s = new ManagedZMQDistSub(key, this, this.srv, this.encrypted);
                this.subscriptions.put(key, s);
            }
            else {
                s = new UnManagedZMQDistSub(key, this, this.srv, this.encrypted);
                this.subscriptions.put(key, s);
            }
        }
        return s;
    }
    
    static {
        ZMQChannel.logger = Logger.getLogger((Class)ZMQChannel.class);
    }
}
