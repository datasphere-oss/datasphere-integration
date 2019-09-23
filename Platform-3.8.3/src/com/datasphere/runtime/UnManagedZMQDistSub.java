package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.exceptions.NodeNotFoundException;
import com.datasphere.uuid.UUID;

public class UnManagedZMQDistSub extends ZMQDistSub implements ItemListener<UUID>, MembershipListener
{
    private static Logger logger;
    private ISet<UUID> peers;
    private String memberShipId;
    private String itemListenerRegisteredId;
    
    public UnManagedZMQDistSub(final DistLink l, final DistributedChannel pStream, final BaseServer srvr, final boolean encrypted) {
        super(l, pStream, srvr, encrypted);
        final String ringName = "STREAM: " + this.streamSubscriberName;
        this.memberShipId = HazelcastSingleton.get().getCluster().addMembershipListener((MembershipListener)this);
        this.peers = HazelcastSingleton.get().getSet("#StreamSubsPeersList-" + pStream.getMetaObject().getUuid() + "-" + this.dist_link.getHash());
        if (UnManagedZMQDistSub.logger.isDebugEnabled()) {
            UnManagedZMQDistSub.logger.debug((Object)("--> DistSub Created: " + ringName));
        }
    }
    
    protected UUID getFirstPeer() {
        if (this.firstPeer == null) {
            final Iterator it = this.peers.iterator();
            if (it.hasNext()) {
                this.firstPeer = (UUID)it.next();
            }
        }
        return this.firstPeer;
    }
    
    public void addPeer(final UUID peer) {
        this.addToPeersList(peer);
        if (UnManagedZMQDistSub.logger.isDebugEnabled()) {
            UnManagedZMQDistSub.logger.debug((Object)("Sending peer request for peer " + peer + " stream " + this.streamSubscriberName));
        }
        this.registerServer(peer);
    }
    
    public void addToPeersList(final UUID peer) {
        this.peers.add(peer);
        this.firstPeer = null;
    }
    
    public int getPeerCount() {
        return this.peers.size();
    }
    
    public String toString() {
        return this.dist_link + " " + Arrays.toString(this.peers.toArray());
    }
    
    public void addDistributedRcvr(final String receiverName, final Stream owner, final DistributedChannel d_channel, final boolean encrypted, final MessagingSystem messagingSystem, final Link link, final Map<UUID, Set<UUID>> thisisimportant) {
        this.distributed_rcvr = new DistributedRcvr(receiverName, owner, d_channel, encrypted, messagingSystem);
        try {
            this.distributed_rcvr.startReceiver(receiverName, new HashMap<Object, Object>());
        }
        catch (Exception e) {
            UnManagedZMQDistSub.logger.error((Object)e.getMessage(), (Throwable)e);
        }
        this.distributed_rcvr.addSubscriber(link, this.dist_link);
    }
    
    public void close(final UUID peerId) {
        this.removeDistributedRcvr();
        this.stop();
        this.removeFromPeersList(peerId);
        HazelcastSingleton.get().getCluster().removeMembershipListener(this.memberShipId);
        if (this.peers.size() == 0) {
            this.peers.destroy();
            this.peers = null;
        }
        this.pStream = null;
        this.dist_link = null;
        this.srvr = null;
    }
    
    public synchronized void start() {
        this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1);
        this.status = Status.STARTED;
        this.itemListenerRegisteredId = this.peers.addItemListener((ItemListener)this, true);
        final Iterator it = this.peers.iterator();
        if (UnManagedZMQDistSub.logger.isDebugEnabled()) {
            UnManagedZMQDistSub.logger.debug((Object)("Peers in the list : " + Arrays.toString(this.peers.toArray())));
        }
        while (it.hasNext()) {
            final UUID peer = (UUID)it.next();
            this.registerServer(peer);
        }
        if (UnManagedZMQDistSub.logger.isInfoEnabled()) {
            UnManagedZMQDistSub.logger.info((Object)("Started subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName()));
        }
    }
    
    public void start(final Map<UUID, Set<UUID>> servers) {
        this.start();
    }
    
    public void startDistributedReceiver(final Map<UUID, Set<UUID>> thisisimportant) {
    }
    
    public void postStopHook() {
        this.peers.removeItemListener(this.itemListenerRegisteredId);
    }
    
    public void itemAdded(final ItemEvent<UUID> uuidItemEvent) {
        final UUID peer = (UUID)uuidItemEvent.getItem();
        this.registerServer(peer);
    }
    
    public void itemRemoved(final ItemEvent<UUID> uuidItemEvent) {
    }
    
    private void registerServer(final UUID peer) {
        try {
            if (this.status != Status.STARTED) {
                UnManagedZMQDistSub.logger.debug((Object)("Stream " + this.streamSubscriberName + " is not started so cannot register peer " + peer));
                return;
            }
            if (this.ring.getNodes().contains(peer)) {
                if (UnManagedZMQDistSub.logger.isDebugEnabled()) {
                    UnManagedZMQDistSub.logger.debug((Object)("node " + peer + " is already registered to stream " + this.streamSubscriberName));
                }
                return;
            }
            this.ring.add(peer);
            if (UnManagedZMQDistSub.logger.isDebugEnabled()) {
                UnManagedZMQDistSub.logger.debug((Object)("Adding node " + peer + " to stream " + this.streamSubscriberName));
            }
        }
        catch (NodeNotFoundException e) {
            this.removeFromPeersList(peer);
        }
    }
    
    private void removeFromPeersList(final UUID peer) {
        this.peers.remove((Object)peer);
        this.firstPeer = null;
    }
    
    private synchronized void deregisterServer(final ItemEvent<UUID> uuidItemEvent) {
        if (this.status == Status.STARTED) {
            return;
        }
        final UUID peer = (UUID)uuidItemEvent.getItem();
        if (this.ring != null) {
            try {
                this.ring.remove(peer);
            }
            catch (NodeNotFoundException e) {
                this.removeFromPeersList(peer);
            }
        }
    }
    
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        final List<UUID> servers = new ArrayList<UUID>();
        for (final Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
            if (entry.getValue() != null && entry.getValue().contains(this.dist_link.getSubID())) {
                servers.add(entry.getKey());
            }
        }
        if (this.ring.getNodes().containsAll(servers)) {
            return true;
        }
        servers.removeAll(this.ring.getNodes());
        if (UnManagedZMQDistSub.logger.isInfoEnabled()) {
            UnManagedZMQDistSub.logger.info((Object)("Failed verifying all the subscription for stream " + this.streamSubscriberName + ". It is still waiting to get subscription from " + servers + ". Peers contain " + Arrays.toString(this.peers.toArray())));
        }
        return false;
    }
    
    public void memberAdded(final MembershipEvent membershipEvent) {
    }
    
    public void memberRemoved(final MembershipEvent membershipEvent) {
        this.removeFromPeersList(new UUID(membershipEvent.getMember().getUuid()));
    }
    
    public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
    }
    
    @Override
    protected Collection<UUID> getAllPeerIds() {
        final Collection<UUID> result = new HashSet<UUID>();
        result.addAll((Collection<? extends UUID>)this.peers);
        return result;
    }
    
    static {
        UnManagedZMQDistSub.logger = Logger.getLogger((Class)UnManagedZMQDistSub.class);
    }
}
