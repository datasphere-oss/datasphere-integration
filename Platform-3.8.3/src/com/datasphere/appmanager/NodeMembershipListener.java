package com.datasphere.appmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.Event;
import com.datasphere.appmanager.event.NodeEventCallback;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.uuid.UUID;

public class NodeMembershipListener implements MembershipListener, ClientListener
{
    private final String clientMembershipRegistrationId;
    NodeEventCallback callback;
    private String eventQueueRegistrationId;
    private static Logger logger;
    private String membershipRegistrationId;
    
    public NodeMembershipListener(final NodeEventCallback callback) {
        this.callback = null;
        this.callback = callback;
        this.membershipRegistrationId = HazelcastSingleton.get().getCluster().addMembershipListener((MembershipListener)this);
        this.clientMembershipRegistrationId = HazelcastSingleton.get().getClientService().addClientListener((ClientListener)this);
    }
    
    public void memberAdded(final MembershipEvent membershipEvent) {
    }
    
    public void memberRemoved(final MembershipEvent membershipEvent) {
        if (this.callback != null) {
            final Member member = membershipEvent.getMember();
            final UUID removedMemberUUID = new UUID(member.getUuid());
            try {
                EventQueueManager.get().addMembershipEventToQueue(Event.EventAction.NODE_DELETED, removedMemberUUID);
            }
            catch (Exception e) {
                NodeMembershipListener.logger.error((Object)("Failed to notify appManager about member removed because of " + e.getMessage()));
            }
        }
    }
    
    public void clientConnected(final Client client) {
    }
    
    public void clientDisconnected(final Client client) {
        if (this.callback != null) {
            final UUID removedMemberUUID = new UUID(client.getUuid());
            try {
                EventQueueManager.get().addMembershipEventToQueue(Event.EventAction.AGENT_NODE_DELETED, removedMemberUUID);
            }
            catch (Exception e) {
                NodeMembershipListener.logger.error((Object)("Failed to notify appManager about member removed because of " + e.getMessage()));
            }
        }
    }
    
    public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
    }
    
    public void close() {
        if (this.membershipRegistrationId != null) {
            HazelcastSingleton.get().getCluster().removeMembershipListener(this.membershipRegistrationId);
        }
        if (this.clientMembershipRegistrationId != null) {
            HazelcastSingleton.get().getClientService().removeClientListener(this.clientMembershipRegistrationId);
        }
    }
    
    public List<UUID> getAllServers() {
        final Set<Member> members = (Set<Member>)HazelcastSingleton.get().getCluster().getMembers();
        final List<UUID> servers = new ArrayList<UUID>();
        for (final Member member : members) {
            servers.add(new UUID(member.getUuid()));
        }
        return servers;
    }
    
    public Event take() {
        return null;
    }
    
    static {
        NodeMembershipListener.logger = Logger.getLogger((Class)NodeMembershipListener.class);
    }
}
