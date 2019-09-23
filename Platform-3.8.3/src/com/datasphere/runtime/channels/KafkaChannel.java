package com.datasphere.runtime.channels;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.datasphere.kafkamessaging.KafkaSender;
import com.datasphere.kafkamessaging.KafkaSenderIntf;
import com.datasphere.messaging.MessagingProvider;
import com.datasphere.metaRepository.MetadataRepositoryUtils;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.DistSub;
import com.datasphere.runtime.KafkaDistSub;
import com.datasphere.runtime.KafkaManagedDistSub;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

import scala.NotImplementedError;

public class KafkaChannel extends DistributedChannel
{
    private static Logger logger;
    private KafkaSenderIntf kafkaSenderIntf;
    public final ConcurrentHashMap<DistLink, KafkaManagedDistSub> subscriptions;
    private Map<UUID, Set<UUID>> app_nodes;
    
    public KafkaChannel(final Stream stream_runtime) throws Exception {
        super(stream_runtime);
        this.subscriptions = new ConcurrentHashMap<DistLink, KafkaManagedDistSub>();
        this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.KAFKA_SYSTEM);
    }
    
    @Override
    public Set<DistLink> getSubscribers() {
        return this.subscriptions.keySet();
    }
    
    @Override
    public synchronized void publish(final ITaskEvent event) throws Exception {
        if (this.status != Status.RUNNING) {
            return;
        }
        if (this.status == Status.RUNNING) {
            if (this.kafkaSenderIntf == null) {
                final boolean is_encrypted = this.isCurrentStreamEncrypted();
                final boolean recoveryEnabled = this.owner.isRecoveryEnabled();
                this.kafkaSenderIntf = new KafkaSender(is_encrypted, this.owner, this.keyFactory, recoveryEnabled);
            }
            this.kafkaSenderIntf.send(event);
        }
    }
    
    @Override
    public void addSubscriber(final Link link) throws Exception {
        final UUID subID = this.getSubscriberID(link.subscriber);
        final String name = this.getSubscriberName(link.subscriber);
        final DistLink key = new DistLink(subID, link.linkID, name);
        final KafkaManagedDistSub dist_sub = this.createGetDistSub(key);
        final String receiver_name = DistributedChannel.buildReceiverName(this.stream_metainfo, subID);
        if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
            KafkaChannel.logger.info((Object)("Added subscriber " + name + " with receiver_name: " + receiver_name));
        }
        dist_sub.addDistributedRcvr(receiver_name, this.owner, this, this.encrypted, this.messagingSystem, link, this.app_nodes);
        if (this.status != Status.RUNNING) {
            throw new RuntimeException("Wrong status when adding subscriber to " + this.stream_metainfo.name + ": expected RUNNING but found " + this.status);
        }
        try {
            dist_sub.startDistributedReceiver(this.app_nodes);
        }
        catch (Exception e) {
            throw e;
        }
        this.subscriptions.put(key, dist_sub);
    }
    
    @Override
    public void removeSubscriber(final Link link) {
        final UUID subID = this.getSubscriberID(link.subscriber);
        final String name = this.getSubscriberName(link.subscriber);
        final DistLink key = new DistLink(subID, link.linkID, name);
        if (key != null) {
            this.subscriptions.remove(key);
        }
    }
    
    @Override
    public void addCallback(final Channel.NewSubscriberAddedCallback callback) {
        throw new NotImplementedError();
    }
    
    @Override
    public int getSubscribersCount() {
        return this.subscriptions.size();
    }
    
    @Override
    public void close() throws IOException {
        try {
            KeyFactory.removeKeyFactory(this.getStreamInfo(), this.srv);
        }
        catch (Exception e) {
            KafkaChannel.logger.error((Object)e.getMessage(), (Throwable)e);
        }
    }
    
    @Override
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        return null;
    }
    
    @Override
    public boolean isFull() {
        return false;
    }
    
    public Map getStats() {
        if (this.kafkaSenderIntf != null) {
            return this.kafkaSenderIntf.getStats();
        }
        return null;
    }
    
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final MetaInfo.PropertySet propertySet = KafkaStreamUtils.getPropertySet(this.stream_metainfo);
        if (propertySet != null) {
            final Map<String, Object> properties = propertySet.getProperties();
            if (properties != null && properties.containsKey("jmx.broker")) {
                monEvs.add(MonitorEvent.Type.KAFKA_BROKERS, properties.get("jmx.broker").toString());
            }
        }
    }
    
    @Override
    public synchronized Position getCheckpoint() {
        final PathManager result = new PathManager();
        if (this.kafkaSenderIntf != null) {
            result.mergeHigherPositions(this.kafkaSenderIntf.getComponentCheckpoint());
        }
        for (final KafkaDistSub kds : this.subscriptions.values()) {
            result.mergeHigherPositions(kds.getComponentCheckpoint());
        }
        return result.toPosition();
    }
    
    @Override
    public synchronized void stop() {
        if (this.status != Status.RUNNING) {
            if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("KafkaChannel " + this.getName() + " is not running so nothing to stop"));
            }
            return;
        }
        if (this.kafkaSenderIntf != null) {
            this.kafkaSenderIntf.stop();
            this.kafkaSenderIntf = null;
        }
        if (this.subscriptions != null && !this.subscriptions.isEmpty()) {
            for (final DistSub sub : this.subscriptions.values()) {
                sub.stop();
            }
        }
        this.status = Status.INITIALIZED;
        if (this.subscriptions.isEmpty()) {
            if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("Started KafkaChannel " + this.getName() + " without subscribers"));
            }
            else if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("Started KafkaChannel " + this.getName() + " with " + this.subscriptions.size() + " subscribers"));
            }
        }
    }
    
    @Override
    public void start() {
        this.start(null);
    }
    
    @Override
    public void start(final Map<UUID, Set<UUID>> servers) {
        if (this.status != Status.INITIALIZED) {
            if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("KafkaChannel " + this.getName() + " is already running so cannot start it again."));
            }
            return;
        }
        this.app_nodes = servers;
        this.status = Status.RUNNING;
        if (this.subscriptions.isEmpty()) {
            if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("Started KafkaChannel " + this.getName() + " without subscribers"));
            }
            else if (KafkaChannel.logger.isInfoEnabled()) {
                KafkaChannel.logger.info((Object)("Started KafkaChannel " + this.getName() + " with " + this.subscriptions.size() + " subscribers"));
            }
        }
    }
    
    @Override
    public boolean verifyStart(final Map<UUID, Set<UUID>> servers) {
        return true;
    }
    
    @Override
    public void closeDistSub(final String uuid) {
    }
    
    private boolean isCurrentStreamEncrypted() {
        final MetaInfo.MetaObject currentStreamInfo = this.owner.getMetaInfo();
        final MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(currentStreamInfo);
        return appInfo != null && appInfo.encrypted;
    }
    
    public static String buildReceiverName(final MetaInfo.Stream stream_metainfo) {
        return KafkaStreamUtils.createTopicName(stream_metainfo);
    }
    
    @Override
    public void startEmitting(final SourcePosition startPosition) throws Exception {
        assert !(!(startPosition instanceof PartitionedSourcePosition));
        for (final KafkaDistSub distSub : this.subscriptions.values()) {
            distSub.startEmitting((PartitionedSourcePosition)startPosition);
        }
    }
    
    private synchronized KafkaManagedDistSub createGetDistSub(final DistLink key) {
        KafkaManagedDistSub s = this.subscriptions.get(key);
        if (s == null) {
            s = new KafkaManagedDistSub(key, this, this.srv, this.encrypted);
            this.subscriptions.put(key, s);
        }
        return s;
    }
    
    static {
        KafkaChannel.logger = Logger.getLogger((Class)KafkaChannel.class);
    }
}
