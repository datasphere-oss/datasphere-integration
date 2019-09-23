package com.datasphere.runtime;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datasphere.kafkamessaging.KafkaSystem;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.Sender;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.uuid.UUID;

public class KafkaManagedDistSub extends KafkaDistSub
{
    private String receiver_name;
    
    public KafkaManagedDistSub(final DistLink key, final DistributedChannel distributedChannelInterface, final BaseServer srv, final boolean encrypted) {
        super(key, distributedChannelInterface, srv, encrypted);
    }
    
    @Override
    protected UUID getFirstPeer() {
        return new UUID(System.currentTimeMillis());
    }
    
    @Override
    public void addDistributedRcvr(final String receiverName, final Stream owner, final DistributedChannel d_channel, final boolean encrypted, final MessagingSystem messagingSystem, final Link link, final Map<UUID, Set<UUID>> thisisimportant) {
        assert messagingSystem instanceof KafkaSystem;
        this.receiver_name = receiverName;
        (this.distributedRcvr = new KafkaDistributedRcvr(receiverName, owner, d_channel, encrypted, (KafkaSystem)messagingSystem)).addSubscriber(link, this.distLink);
        this.distributedRcvr.addServerList(thisisimportant);
    }
    
    @Override
    public Sender getSender(final int partitionId, final int sendQueueId) throws InterruptedException {
        return null;
    }
    
    @Override
    public void start() {
        if (this.status != Status.STARTED) {
            this.status = Status.STARTED;
        }
        if (KafkaManagedDistSub.logger.isInfoEnabled()) {
            KafkaManagedDistSub.logger.info((Object)("Started subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName()));
        }
    }
    
    @Override
    public void start(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        if (this.status != Status.STARTED) {
            this.status = Status.STARTED;
        }
        if (KafkaManagedDistSub.logger.isInfoEnabled()) {
            KafkaManagedDistSub.logger.info((Object)("Started subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName()));
        }
    }
    
    @Override
    public void startDistributedReceiver(final Map<UUID, Set<UUID>> thisisimportant) throws Exception {
        if (this.distributedRcvr != null) {
            this.distributedRcvr.startReceiver(this.receiver_name, new HashMap<Object, Object>());
        }
    }
    
    @Override
    public void postStopHook() {
    }
    
    @Override
    public void close(final UUID peerID) {
        this.stop();
        this.parentStream = null;
        this.distLink = null;
        this.srvr = null;
    }
    
    @Override
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        return true;
    }
    
    @Override
    public void addPeer(final UUID thisPeerId) {
    }
    
    @Override
    public int getPeerCount() {
        return 0;
    }
}
