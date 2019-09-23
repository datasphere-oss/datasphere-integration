package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.zeromq.ZMQException;

import com.datasphere.kafkamessaging.KafkaSystem;
import com.hazelcast.core.IQueue;
import com.datasphere.messaging.InterThreadComm;
import com.datasphere.messaging.Sender;
import com.datasphere.messaging.SocketType;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.channels.KafkaChannel;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.exceptions.NodeNotFoundException;
import com.datasphere.uuid.UUID;

import scala.NotImplementedError;
import zmq.ZError;

public abstract class ZMQDistSub extends DistSub implements ChannelEventHandler
{
    protected static Logger logger;
    protected final String streamSubscriberName;
    public DistLink dist_link;
    protected BaseServer srvr;
    protected DistributedChannel pStream;
    protected final boolean encrypted;
    protected ConsistentHashRing ring;
    protected DistributedRcvr distributed_rcvr;
    final Map<UUID, Map<Integer, Sender>> senderMap;
    public Status status;
    private IQueue<String> evTS;
    protected long unProcessed;
    protected final boolean enableParallelSender = false;
    protected UUID firstPeer;
    int numSenderThreads;
    ArrayList<InterThreadComm> queues;
    private final String recoveryTopicName;
    private String positionRequestMessageListenerRegId;
    protected boolean debugEventCountMatch;
    protected boolean debugEventLoss;
    Map<UUID, Integer> peerCounts;
    private ReentrantLock stopLock;
    
    public ZMQDistSub(final DistLink l, final DistributedChannel pStream, final BaseServer srvr, final boolean encrypted) {
        this.evTS = null;
        this.unProcessed = 0L;
        this.numSenderThreads = new Integer(System.getProperty("com.datasphere.config.sendCapacity", "1"));
        this.queues = new ArrayList<InterThreadComm>(this.numSenderThreads);
        this.positionRequestMessageListenerRegId = null;
        this.debugEventCountMatch = false;
        this.debugEventLoss = false;
        this.peerCounts = new ConcurrentHashMap<UUID, Integer>();
        this.dist_link = l;
        this.srvr = srvr;
        this.pStream = pStream;
        this.encrypted = encrypted;
        this.streamSubscriberName = pStream.getMetaObject().getName() + "-" + this.dist_link.getName();
        this.senderMap = new ConcurrentHashMap<UUID, Map<Integer, Sender>>();
        this.status = Status.INITIALIZED;
        if (this.debugEventLoss) {
            ZMQDistSub.logger.debug((Object)("id->name " + this.dist_link.getSubID() + " = " + this.dist_link.getName()));
            this.evTS = HazelcastSingleton.get().getQueue(pStream.getMetaObject().getUuid() + "-" + this.dist_link.getSubID() + "-evTS");
        }
        this.recoveryTopicName = "#StreamConfirm_" + this.pStream.getMetaObject().getUuid();
        this.stopLock = new ReentrantLock();
    }
    
    @Override
    public void sendEvent(final EventContainer container, final int sendQueueId, final LagMarker islagmarker) throws InterruptedException {
        final Sender sender = container.getDistSub().getSender(container.getPartitionId(), sendQueueId);
        if (sender == null) {
            if (ZMQDistSub.logger.isDebugEnabled()) {
                ZMQDistSub.logger.debug((Object)("Got null Sender so dropping message for stream " + this.streamSubscriberName));
            }
            return;
        }
        final DARecord DARecord = (DARecord)container.getEvent();
        final TaskEvent taskEvent = (TaskEvent)StreamEventFactory.createStreamEvent(DARecord.data, DARecord.position);
        if (islagmarker != null && !islagmarker.isEmpty()) {
            taskEvent.setLagRecord(true);
            taskEvent.setLagMarker(islagmarker.copy());
        }
        final StreamEvent streamEvent = new StreamEvent((ITaskEvent)taskEvent, container.getDistSub().dist_link);
        try {
            final boolean success = sender.send(streamEvent);
            if (!success && ZMQDistSub.logger.isDebugEnabled()) {
                ZMQDistSub.logger.debug((Object)("Failed to send message on stream " + this.streamSubscriberName));
            }
        }
        catch (Throwable t) {
            if (t instanceof ZError.IOException) {
                throw new InterruptedException();
            }
            t.printStackTrace();
        }
    }
    
    public synchronized void sendPartitioned(DARecord DARecord, final RecordKey key, final LagMarker islagmarker) throws InterruptedException {
        if (this.status != Status.STARTED || this.ring.size() == 0) {
            this.logDroppedEvents();
        }
        else {
            final int partitionId = this.getPartitionId(key);
            try {
                if (DARecord.position != null) {
                    final DARecord augmentedDARecord = (DARecord)DARecord.getClass().newInstance();
                    augmentedDARecord.initValues(DARecord);
                    augmentedDARecord.position = augmentedDARecord.position.createAugmentedPosition(this.pStream.getMetaObject().getUuid(), String.valueOf(partitionId));
                    DARecord = augmentedDARecord;
                }
            }
            catch (InstantiationException | IllegalAccessException ex2) {
                ZMQDistSub.logger.error((Object)ex2);
            }
            final EventContainer container = new EventContainer();
            container.setAllFields(DARecord, this, partitionId, this);
            this.sendEvent(container, 0, islagmarker);
            if (this.debugEventLoss) {
                this.evTS.add(DARecord.toString());
            }
            if (this.debugEventCountMatch) {
                final UUID peerID = this.getPeerId(partitionId);
                final Integer peerCount = this.peerCounts.get(peerID);
                this.peerCounts.put(peerID, (peerCount == null) ? 1 : (peerCount + 1));
            }
        }
    }
    
    private void logDroppedEvents() {
        ++this.unProcessed;
        if (this.ring != null) {
            if (ZMQDistSub.logger.isDebugEnabled()) {
                ZMQDistSub.logger.debug((Object)("Unprocessed events: " + this.unProcessed + " on stream " + this.pStream.getMetaObject().getName() + " for subscriber " + this.dist_link.getName() + ", status: " + this.status.name() + ", ring size : " + this.ring.size()));
            }
        }
        else if (ZMQDistSub.logger.isDebugEnabled()) {
            ZMQDistSub.logger.debug((Object)("Unprocessed events: " + this.unProcessed + " on stream " + this.pStream.getMetaObject().getName() + " for subscriber " + this.dist_link.getName() + ", status: " + this.status.name()));
        }
    }
    
    public synchronized void sendUnpartitioned(ITaskEvent taskEvent, final UUID localPeerId, final int taskEventSends) throws InterruptedException {
        if (this.status != Status.STARTED || this.ring.size() == 0) {
            this.logDroppedEvents();
        }
        else {
            final boolean here = this.hasThisPeer(localPeerId);
            final UUID peerID = here ? localPeerId : this.getFirstPeer();
            final Sender sender = this.getSender(peerID, 0);
            if (sender == null) {
                if (ZMQDistSub.logger.isDebugEnabled() && peerID != null) {
                    ZMQDistSub.logger.warn((Object)("Got null Sender for " + peerID + " here " + here + " localPeerId " + localPeerId + " so dropping message for stream " + this.streamSubscriberName));
                }
                return;
            }
            boolean needDupeEvent = false;
            if (taskEventSends > 1) {
                for (final DARecord DARecord : taskEvent.batch()) {
                    if (DARecord.position != null) {
                        needDupeEvent = true;
                        break;
                    }
                }
            }
            if (needDupeEvent) {
                taskEvent = (ITaskEvent)TaskEvent.createStreamEventWithNewDARecords(taskEvent);
            }
            for (final DARecord DARecord : taskEvent.batch()) {
                if (DARecord.position != null) {
                    final Position augmentedPosition = DARecord.position.createAugmentedPosition(this.pStream.getMetaObject().getUuid(), (String)null);
                    DARecord.position = augmentedPosition;
                }
            }
            final StreamEvent ae = new StreamEvent(taskEvent, this.dist_link);
            try {
                final boolean success = sender.send(ae);
                if (!success && ZMQDistSub.logger.isDebugEnabled()) {
                    ZMQDistSub.logger.debug((Object)("Failed to send messages on stream " + this.streamSubscriberName));
                }
            }
            catch (ZMQException t) {
                ZMQDistSub.logger.error((Object)"Message queue error", (Throwable)t);
            }
            if (this.debugEventLoss) {
                for (final DARecord DARecord2 : taskEvent.batch()) {
                    this.evTS.add(DARecord2.toString());
                }
            }
            if (this.debugEventCountMatch) {
                final Integer peerCount = this.peerCounts.get(peerID);
                this.peerCounts.put(peerID, (peerCount == null) ? 1 : (peerCount + 1));
            }
        }
    }
    
    @Override
    public void stop() {
        try {
            this.stopLock.lock();
            if (this.status != Status.STARTED) {
                if (ZMQDistSub.logger.isDebugEnabled()) {
                    ZMQDistSub.logger.debug((Object)("stream subscriber " + this.streamSubscriberName + " is not started so not stopping"));
                }
                return;
            }
            if (this.unProcessed > 0L) {
                ZMQDistSub.logger.warn((Object)("Stream " + this.streamSubscriberName + " discarded " + this.unProcessed + " events before it was started"));
                this.unProcessed = 0L;
            }
            for (final Map<Integer, Sender> senders : this.senderMap.values()) {
                for (final Sender ss : senders.values()) {
                    ss.stop();
                }
                senders.clear();
            }
            if (this.distributed_rcvr != null) {
                this.distributed_rcvr.stop();
            }
            this.senderMap.clear();
            this.status = Status.STOPPED;
            this.ring.shutDown();
            this.ring = null;
            if (this.debugEventCountMatch) {
                ZMQDistSub.logger.warn((Object)(this.streamSubscriberName + ": Current PeerCounts: " + this.peerCounts));
                this.peerCounts.clear();
            }
            this.postStopHook();
            ZMQDistSub.logger.info((Object)("Stopped subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getMetaObject().getFullName()));
        }
        catch (Exception e) {
            ZMQDistSub.logger.error((Object)e);
        }
        finally {
            this.stopLock.unlock();
        }
    }
    
    public int getPartitionId(final Object key) {
        return this.ring.getPartitionId(key);
    }
    
    public boolean hasThisPeer(final UUID thisPeerId) {
        return this.ring != null && this.ring.getNodes().contains(thisPeerId);
    }
    
    public UUID getPeerId(final int partitionId) {
        return this.ring.getUUIDForPartition(partitionId, 0);
    }
    
    @Override
    public Sender getSender(final int partitionId, final int senderThreadId) throws InterruptedException {
        final UUID peerId = this.getPeerId(partitionId);
        return this.getSender(peerId, senderThreadId);
    }
    
    private Sender getSender(final UUID peerId, final int senderThreadId) throws InterruptedException {
        if (peerId == null) {
            return null;
        }
        Map<Integer, Sender> senders = this.senderMap.get(peerId);
        if (senders == null) {
            senders = new HashMap<Integer, Sender>();
            this.senderMap.put(peerId, senders);
        }
        Sender sender = null;
        if (senderThreadId < senders.size()) {
            sender = senders.get(senderThreadId);
        }
        if (sender == null) {
            String receiver_name = null;
            try {
                if (this.pStream.messagingSystem instanceof KafkaSystem) {
                    receiver_name = KafkaChannel.buildReceiverName(this.pStream.getMetaObject());
                }
                else {
                    receiver_name = DistributedChannel.buildReceiverName(this.pStream.getMetaObject(), this.dist_link.getSubID());
                }
                if (this.canReceiveDataSynchronously()) {
                    sender = this.pStream.messagingSystem.getConnectionToReceiver(peerId, receiver_name, SocketType.PUSH, this.encrypted, false);
                }
                else {
                    sender = this.pStream.messagingSystem.getConnectionToReceiver(peerId, receiver_name, SocketType.PUSH, this.encrypted, true);
                }
            }
            catch (NodeNotFoundException e) {
                ZMQDistSub.logger.warn((Object)("Could not find node with UUID: " + peerId + " on which receiver " + receiver_name + " is expected to run"), (Throwable)e);
            }
            if (sender != null) {
                senders.put(senderThreadId, sender);
            }
        }
        return sender;
    }
    
    public void removeDistributedRcvr() {
        if (this.distributed_rcvr != null) {
            this.distributed_rcvr.close();
        }
    }
    
    public boolean isFull() {
        final Map<Integer, Sender> senders = this.senderMap.get(HazelcastSingleton.getNodeId());
        if (senders != null) {
            for (final Sender sender : senders.values()) {
                if (sender.isFull()) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @Override
    public void startEmitting(final Position position) {
        throw new NotImplementedError();
    }
    
    public void sendCommand(final CommandEvent commandEvent, final UUID localPeerId) throws InterruptedException {
        if (this.status != Status.STARTED || this.ring.size() == 0) {
            this.logDroppedEvents();
            return;
        }
        final Collection<UUID> allPeerIds = this.getAllPeerIds();
        for (final UUID peerId : allPeerIds) {
            final boolean here = this.hasThisPeer(peerId);
            final Sender sender = this.getSender(peerId, 0);
            if (sender == null) {
                if (!ZMQDistSub.logger.isDebugEnabled() || peerId == null) {
                    continue;
                }
                ZMQDistSub.logger.debug((Object)("Got null Sender for " + peerId + " here " + here + " localPeerId " + localPeerId + " so dropping message for stream " + this.streamSubscriberName));
            }
            else {
                try {
                    final StreamEvent streamEvent = new StreamEvent((ITaskEvent)commandEvent, this.dist_link);
                    final boolean success = sender.send(streamEvent);
                    if (success || !ZMQDistSub.logger.isDebugEnabled()) {
                        continue;
                    }
                    ZMQDistSub.logger.debug((Object)("Failed to send messages on stream " + this.streamSubscriberName));
                }
                catch (ZMQException t) {
                    ZMQDistSub.logger.error((Object)"Message queue error", (Throwable)t);
                }
            }
        }
    }
    
    protected abstract Collection<UUID> getAllPeerIds();
    
    static {
        ZMQDistSub.logger = Logger.getLogger((Class)ZMQDistSub.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        STARTED, 
        STOPPED;
    }
}
