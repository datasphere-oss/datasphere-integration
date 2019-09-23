package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasphere.kafkamessaging.KafkaSystem;
import com.hazelcast.core.ISet;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.channels.KafkaChannel;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.uuid.UUID;

public class ManagedZMQDistSub extends ZMQDistSub
{
    private ISet<UUID> peersFoundByAddingToISet;
    private Set<UUID> peersPassedDownFromAppManager;
    
    public ManagedZMQDistSub(final DistLink key, final DistributedChannel distributedChannelInterface, final BaseServer srv, final boolean encrypted) {
        super(key, distributedChannelInterface, srv, encrypted);
        this.peersFoundByAddingToISet = HazelcastSingleton.get().getSet("#StreamSubsPeersList-" + this.pStream.getMetaObject().getUuid() + "-" + this.dist_link.getHash());
        this.peersPassedDownFromAppManager = new HashSet<UUID>();
    }
    
    @Override
    protected UUID getFirstPeer() {
        if (this.firstPeer == null) {
            Iterator it;
            if (!(Server.getBaseServer() instanceof Server)) {
                it = this.peersFoundByAddingToISet.iterator();
            }
            else {
                it = this.peersPassedDownFromAppManager.iterator();
            }
            if (it.hasNext()) {
                this.firstPeer = (UUID)it.next();
            }
        }
        return this.firstPeer;
    }
    
    @Override
    public void addDistributedRcvr(final String receiverName, final Stream owner, final DistributedChannel d_channel, final boolean encrypted, final MessagingSystem messagingSystem, final Link link, final Map<UUID, Set<UUID>> thisisimportant) {
        (this.distributed_rcvr = new DistributedRcvr(receiverName, owner, d_channel, encrypted, messagingSystem)).addSubscriber(link, this.dist_link);
    }
    
    @Override
    public void start() {
        if (this.status != Status.STARTED) {
            if (!(Server.getBaseServer() instanceof Server)) {
                this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1);
                final List<UUID> servers = new ArrayList<UUID>((Collection<? extends UUID>)this.peersFoundByAddingToISet);
                this.ring.set(servers);
                this.status = Status.STARTED;
                if (ManagedZMQDistSub.logger.isInfoEnabled()) {
                    ManagedZMQDistSub.logger.info((Object)("Starting subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName() + " on AGENT"));
                }
            }
            else {
                ManagedZMQDistSub.logger.warn((Object)("Unexpected point while starting subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName()));
                Thread.dumpStack();
            }
        }
    }
    
    @Override
    public void start(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        if (this.status != Status.STARTED) {
            final List<UUID> servers = new ArrayList<UUID>();
            for (final Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
                if (entry.getValue() != null && entry.getValue().contains(this.dist_link.getSubID())) {
                    servers.add(entry.getKey());
                }
            }
            this.peersPassedDownFromAppManager.addAll(servers);
            (this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1)).set(servers);
            if (ManagedZMQDistSub.logger.isInfoEnabled()) {
                ManagedZMQDistSub.logger.info((Object)("Servers in ring for stream " + this.streamSubscriberName + ": " + this.ring.getNodes()));
            }
            this.status = Status.STARTED;
            if (ManagedZMQDistSub.logger.isInfoEnabled()) {
                ManagedZMQDistSub.logger.info((Object)("Started subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName()));
            }
        }
    }
    
    @Override
    public void startDistributedReceiver(final Map<UUID, Set<UUID>> thisisimportant) {
        if (this.distributed_rcvr != null) {
            String receiver_name;
            if (super.pStream.messagingSystem instanceof KafkaSystem) {
                receiver_name = KafkaChannel.buildReceiverName(this.pStream.getMetaObject());
            }
            else {
                receiver_name = DistributedChannel.buildReceiverName(this.pStream.getMetaObject(), this.dist_link.getSubID());
            }
            try {
                this.distributed_rcvr.startReceiver(receiver_name, new HashMap<Object, Object>());
            }
            catch (Exception e) {
                ManagedZMQDistSub.logger.error((Object)e.getMessage(), (Throwable)e);
            }
        }
    }
    
    @Override
    public void postStopHook() {
    }
    
    @Override
    public void close(final UUID peerID) {
        this.removeDistributedRcvr();
        super.stop();
        this.peersFoundByAddingToISet.remove((Object)peerID);
        this.firstPeer = null;
        this.pStream = null;
        this.dist_link = null;
        this.srvr = null;
    }
    
    @Override
    public void stop() {
        this.peersPassedDownFromAppManager.clear();
        this.firstPeer = null;
        super.stop();
    }
    
    @Override
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        return true;
    }
    
    @Override
    public void addPeer(final UUID thisPeerId) {
        this.peersFoundByAddingToISet.add(thisPeerId);
        if (!(Server.getBaseServer() instanceof Server)) {
            this.resetRing();
        }
    }
    
    private void resetRing() {
        final List<UUID> servers = new ArrayList<UUID>((Collection<? extends UUID>)this.peersFoundByAddingToISet);
        if (this.ring != null) {
            this.ring.set(servers);
        }
    }
    
    @Override
    public int getPeerCount() {
        return 0;
    }
    
    @Override
    protected Collection<UUID> getAllPeerIds() {
        final Collection<UUID> result = new HashSet<UUID>();
        result.addAll((Collection<? extends UUID>)this.peersFoundByAddingToISet);
        result.addAll(this.peersPassedDownFromAppManager);
        return result;
    }
}
