package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.kafkamessaging.KafkaSystem;
import com.datasphere.messaging.Handler;
import com.datasphere.messaging.UnknownObjectException;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.proc.records.PublishableRecord;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class KafkaDistributedRcvr implements Handler
{
    private static Logger logger;
    private final KafkaSystem messagingSystem;
    private final FlowComponent owner;
    private final DistributedChannel channel;
    private final String receiverName;
    private Status status;
    private boolean recoveryEnabled;
    private Link subscriber_link;
    private DistLink subscriber_distlink;
    private List<Integer> partitions_i_own;
    
    public KafkaDistributedRcvr(final String receiverName, final FlowComponent owner, final DistributedChannel pStream, final boolean encrypted, final KafkaSystem messagingSystem) {
        this.partitions_i_own = new ArrayList<Integer>();
        this.messagingSystem = messagingSystem;
        this.owner = owner;
        this.channel = pStream;
        this.status = Status.INITIALIZED;
        this.receiverName = receiverName;
        this.recoveryEnabled = (this.owner != null && this.owner.getFlow() != null && this.owner.getFlow().recoveryIsEnabled());
        this.messagingSystem.createReceiver(messagingSystem.getReceiverClass(), this, receiverName, encrypted, pStream.getOwner().getMetaInfo());
    }
    
    public boolean isRecoveryEnabled() {
        return this.recoveryEnabled;
    }
    
    @Override
    public String getName() {
        return this.receiverName;
    }
    
    @Override
    public FlowComponent getOwner() {
        return this.owner;
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
        else if (data instanceof CommandEvent) {
            final CommandEvent commandEvent = (CommandEvent)data;
            this.doReceive((ITaskEvent)commandEvent);
        }
        else if (data != null) {
            throw new UnknownObjectException("Expecting Stream Event at " + this.owner.getMetaName() + ", got : " + data.getClass().toString());
        }
    }
    
    private void doReceive(final ITaskEvent taskEvent) {
        try {
            if (taskEvent instanceof CommandEvent) {
                final CommandEvent commandEvent = (CommandEvent)taskEvent;
                try {
                    if (!commandEvent.performCommandForStream(this.getOwner(), this.subscriber_link.subscriber.hashCode())) {
                        return;
                    }
                    KafkaDistributedRcvr.logger.info((Object)("Sending command event to subscriber " + this.subscriber_link.subscriber.getClass().getName()));
                }
                catch (Exception e) {
                    KafkaDistributedRcvr.logger.error((Object)e.getMessage(), (Throwable)e);
                }
            }
            this.subscriber_link.subscriber.receive(this.subscriber_link.linkID, taskEvent);
            this.debugEvents(this.subscriber_distlink, taskEvent);
        }
        catch (Exception e2) {
            KafkaDistributedRcvr.logger.error((Object)"Error receiving event", (Throwable)e2);
        }
    }
    
    public void doReceive(final DistLink link, final ITaskEvent event) {
        if (this.recoveryEnabled) {
            final PathManager batchHighPosition = new PathManager();
            for (final DARecord ev : event.batch()) {
                batchHighPosition.mergeHigherPositions(ev.position);
            }
        }
        if (this.subscriber_link != null) {
            try {
                this.subscriber_link.subscriber.receive(this.subscriber_link.linkID, event);
                this.debugEvents(link, event);
            }
            catch (Exception e) {
                KafkaDistributedRcvr.logger.error((Object)"Error receiving event", (Throwable)e);
            }
        }
        else if (KafkaDistributedRcvr.logger.isDebugEnabled()) {
            KafkaDistributedRcvr.logger.debug((Object)(this.channel.getDebugId() + " tried to pass event to unknown subscriber " + link));
        }
    }
    
    public void addSubscriber(final Link link, final DistLink key) {
        this.subscriber_link = link;
        this.subscriber_distlink = key;
    }
    
    private void debugEvents(final DistLink subscriber_distlink, final ITaskEvent taskEvent) {
        if (KafkaDistributedRcvr.logger.isDebugEnabled()) {
            KafkaDistributedRcvr.logger.error((Object)"I am not yet debugging this");
        }
    }
    
    public void addServerList(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        final List<UUID> servers = new ArrayList<UUID>();
        for (final Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
            if (entry.getValue() != null && entry.getValue().contains(this.subscriber_distlink.getSubID())) {
                servers.add(entry.getKey());
            }
        }
        this.createPartitions(servers);
    }
    
    private void createPartitions(final List<UUID> servers) {
        final MetaInfo.Stream streamInfo = this.channel.stream_metainfo;
        final MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(streamInfo);
        final int partitions = KafkaStreamUtils.getPartitionsCount(streamInfo, streamPropset);
        final SimplePartitionManager smp = new SimplePartitionManager(this.receiverName, 1, partitions);
        smp.setServers(servers);
        final UUID this_server_uuid = BaseServer.getBaseServer().getServerID();
        for (int ii = 0; ii < partitions; ++ii) {
            final UUID node_uuid = smp.getFirstPartitionOwnerForPartition(ii);
            if (this_server_uuid.equals((Object)node_uuid)) {
                this.partitions_i_own.add(ii);
            }
        }
    }
    
    public void startReceiver(final String name, final Map<Object, Object> properties) throws Exception {
        if (this.status == Status.INITIALIZED) {
            if (KafkaDistributedRcvr.logger.isInfoEnabled()) {
                KafkaDistributedRcvr.logger.info((Object)(this.getName() + " owns " + this.partitions_i_own.size() + " partitions: " + this.partitions_i_own));
            }
            properties.put("partitions_i_own", this.partitions_i_own);
            this.messagingSystem.startReceiver(name, properties);
            this.status = Status.RUNNING;
        }
    }
    
    public void startEmitting(final String name, final PartitionedSourcePosition startPosition) throws Exception {
        this.messagingSystem.startReceiverForEmitting(name, startPosition);
    }
    
    public void stop() {
        try {
            this.messagingSystem.stopReceiver(this.receiverName);
        }
        catch (Exception ex) {}
        this.status = Status.INITIALIZED;
    }
    
    public Position getComponentCheckpoint() {
        return this.messagingSystem.getComponentCheckpoint();
    }
    
    static {
        KafkaDistributedRcvr.logger = Logger.getLogger((Class)KafkaDistributedRcvr.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        RUNNING;
    }
}
