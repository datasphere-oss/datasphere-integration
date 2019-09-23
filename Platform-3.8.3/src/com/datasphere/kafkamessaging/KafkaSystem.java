package com.datasphere.kafkamessaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.datasphere.jmqmessaging.KafkaReceiverInfo;
import com.hazelcast.core.RuntimeInterruptedException;
import com.datasphere.kafka.KafkaException;
import com.datasphere.messaging.Handler;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.NoDataConsumerException;
import com.datasphere.messaging.Receiver;
import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.messaging.Sender;
import com.datasphere.messaging.SocketType;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.exceptions.NodeNotFoundException;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class KafkaSystem implements MessagingSystem
{
    private static Logger logger;
    private final UUID serverID;
    private final Map<UUID, List<ReceiverInfo>> HostToReceiverInfo;
    private Map<String, KafkaReceiver> receiversInThisSystem;
    
    public KafkaSystem(final UUID serverID, final String paramNotUsed) {
        this.serverID = serverID;
        synchronized (this.HostToReceiverInfo = HazelcastSingleton.get().getMap("#HostToMessengerMap")) {
            if (!this.HostToReceiverInfo.containsKey(this.serverID)) {
                this.HostToReceiverInfo.put(this.serverID, new ArrayList<ReceiverInfo>());
            }
        }
        this.receiversInThisSystem = new ConcurrentHashMap<String, KafkaReceiver>();
        if (KafkaSystem.logger.isInfoEnabled()) {
            KafkaSystem.logger.info((Object)("Kafka System Initialized: ID=" + serverID));
        }
    }
    
    @Override
    public void createReceiver(final Class clazz, final Handler handler, final String name, final boolean encrypted, final MetaInfo.Stream streamInfo) {
        if (clazz == null) {
            throw new NullPointerException("Can't create a Receiver out of a Null Class!");
        }
        if (name == null) {
            throw new NullPointerException("Name of a Receiver can't be Null!");
        }
        if (KafkaSystem.logger.isDebugEnabled()) {
            KafkaSystem.logger.debug((Object)("Creating receiver for " + name));
        }
        final KafkaReceiverInfo info = new KafkaReceiverInfo(name, KafkaStreamUtils.makeSafeKafkaTopicName(streamInfo.getFullName()));
        final KafkaReceiver receiver = new KafkaReceiver(info, streamInfo, handler, encrypted);
        this.receiversInThisSystem.put(receiver.name, receiver);
    }
    
    @Override
    public Receiver getReceiver(final String name) {
        return this.receiversInThisSystem.get(name);
    }
    
    @Override
    public void startReceiver(final String name, final Map<Object, Object> properties) throws Exception {
        final KafkaReceiver kafkaReceiver = this.receiversInThisSystem.get(name);
        if (kafkaReceiver != null) {
            kafkaReceiver.start(properties);
            this.updateReceiverMap(kafkaReceiver);
            return;
        }
        throw new KafkaException(name + " Receiver is not created.");
    }
    
    public void startReceiverForEmitting(final String name, final PartitionedSourcePosition startPosition) throws Exception {
        final KafkaReceiver kafkaReceiver = this.receiversInThisSystem.get(name);
        if (kafkaReceiver != null) {
            if (startPosition != null) {
                kafkaReceiver.setPosition(startPosition);
            }
            kafkaReceiver.startForEmitting();
            this.updateReceiverMap(kafkaReceiver);
            return;
        }
        throw new NullPointerException(name + " Receiver is not created.");
    }
    
    @Override
    public Sender getConnectionToReceiver(final UUID peerID, final String name, final SocketType type, final boolean isEncrypted, final boolean async) throws NodeNotFoundException, InterruptedException {
        if (peerID == null) {
            throw new NoDataConsumerException("PeerID is null, can't find component " + name + " on null.");
        }
        if (name == null) {
            throw new NullPointerException("Component name is null, looking for component with name null on WAServer with ID : " + peerID + " is not possible.");
        }
        if (type == null) {
            throw new NullPointerException("Socket type is null, can't create a Sender to " + name + " on  WAServer with ID : " + peerID);
        }
        final ReceiverInfo rinfo = this.getReceiverInfo(peerID, name);
        if (!(rinfo instanceof KafkaReceiverInfo)) {
            throw new NodeNotFoundException(name + " is not a KafkaReceiverInfo, it is a " + rinfo.getClass());
        }
        final KafkaReceiverInfo info = (KafkaReceiverInfo)rinfo;
        if (KafkaSystem.logger.isTraceEnabled()) {
            KafkaSystem.logger.trace((Object)("Sender for " + name + " on peer: " + peerID + " created"));
        }
        return null;
    }
    
    public void updateReceiverMap(final KafkaReceiver rcvr) {
        if (rcvr == null) {
            throw new NullPointerException("Receiver can't be null!");
        }
        synchronized (this.HostToReceiverInfo) {
            final List<ReceiverInfo> kafkaReceiverInfoList = this.HostToReceiverInfo.get(this.serverID);
            final KafkaReceiverInfo info = new KafkaReceiverInfo(rcvr.name, KafkaStreamUtils.makeSafeKafkaTopicName(rcvr.name));
            if (this.validateCorrectness(kafkaReceiverInfoList, info)) {
                kafkaReceiverInfoList.add((ReceiverInfo)info);
                this.HostToReceiverInfo.put(this.serverID, kafkaReceiverInfoList);
                if (KafkaSystem.logger.isTraceEnabled()) {
                    KafkaSystem.logger.trace((Object)("Kafka Messengers on current(" + this.serverID + ") Kafka System : \n" + this.receiversInThisSystem.keySet()));
                }
            }
            else if (KafkaSystem.logger.isDebugEnabled()) {
                KafkaSystem.logger.debug((Object)("Receiver " + rcvr.name + " already exists"));
            }
        }
    }
    
    public boolean validateCorrectness(final List<ReceiverInfo> kafkaReceiverInfoList, final KafkaReceiverInfo info) {
        if (info == null) {
            throw new NullPointerException("Can't validate correctness of a Null Object!");
        }
        if (kafkaReceiverInfoList.isEmpty()) {
            return true;
        }
        for (final ReceiverInfo mInfo : kafkaReceiverInfoList) {
            if (mInfo.equals(info)) {
                return false;
            }
        }
        return true;
    }
    
    private ReceiverInfo getReceiverInfo(final UUID uuid, final String name) throws InterruptedException {
        ReceiverInfo info = null;
        int count = 0;
        while (info == null) {
            synchronized (this.HostToReceiverInfo) {
                List<ReceiverInfo> returnInfo;
                try {
                    returnInfo = this.HostToReceiverInfo.get(uuid);
                }
                catch (RuntimeInterruptedException e) {
                    throw new InterruptedException(e.getMessage());
                }
                if (returnInfo != null && !returnInfo.isEmpty()) {
                    for (final ReceiverInfo anInfo : returnInfo) {
                        if (name.equals(anInfo.getName())) {
                            info = anInfo;
                        }
                    }
                }
            }
            if (info != null || count >= 10) {
                break;
            }
            Thread.sleep(500L);
            ++count;
        }
        return info;
    }
    
    @Override
    public boolean stopReceiver(final String name) throws Exception {
        if (name == null) {
            throw new NullPointerException("Can't Undeploy a Receiver with Null Value!");
        }
        final boolean isAlive = this.killReceiver(name);
        synchronized (this.HostToReceiverInfo) {
            final ArrayList<ReceiverInfo> rcvrs = (ArrayList<ReceiverInfo>)this.HostToReceiverInfo.get(this.serverID);
            boolean removed = false;
            for (final ReceiverInfo rcvrInfo : rcvrs) {
                if (name.equalsIgnoreCase(rcvrInfo.getName())) {
                    removed = rcvrs.remove(rcvrInfo);
                    break;
                }
            }
            if (isAlive || !removed) {
                throw new RuntimeException("Receiver : " + name + " not found");
            }
            if (KafkaSystem.logger.isDebugEnabled()) {
                KafkaSystem.logger.debug((Object)("Stopped Receiver : " + name));
            }
            this.HostToReceiverInfo.put(this.serverID, rcvrs);
            this.receiversInThisSystem.remove(name);
        }
        if (KafkaSystem.logger.isTraceEnabled()) {
            KafkaSystem.logger.trace((Object)("HostToReceiver : " + this.HostToReceiverInfo.entrySet()));
            KafkaSystem.logger.trace((Object)("All receievers : " + this.receiversInThisSystem.entrySet()));
        }
        return isAlive;
    }
    
    public boolean killReceiver(final String name) throws Exception {
        final KafkaReceiver messenger = this.receiversInThisSystem.get(name);
        if (messenger != null) {
            boolean isAlive = messenger.stop();
            while (isAlive) {
                isAlive = messenger.stop();
                if (KafkaSystem.logger.isDebugEnabled()) {
                    KafkaSystem.logger.debug((Object)("Trying to stop receiver : " + name));
                }
                Thread.sleep(100L);
            }
            if (KafkaSystem.logger.isDebugEnabled()) {
                KafkaSystem.logger.debug((Object)("Stopped Receiver : " + name));
            }
            return isAlive;
        }
        throw new NullPointerException(name + " Receiver does not exist");
    }
    
    public boolean stopAllReceivers() {
        boolean isAlive = false;
        for (final String name : this.receiversInThisSystem.keySet()) {
            try {
                isAlive = this.stopReceiver(name);
            }
            catch (Exception e) {
                KafkaSystem.logger.debug((Object)e.getMessage());
            }
        }
        return isAlive;
    }
    
    @Override
    public void shutdown() {
        boolean isAlive = this.stopAllReceivers();
        while (isAlive) {
            try {
                Thread.sleep(200L);
                isAlive = this.stopAllReceivers();
            }
            catch (InterruptedException e) {
                if (KafkaSystem.logger.isDebugEnabled()) {
                    KafkaSystem.logger.debug((Object)"Interrupted", (Throwable)e);
                }
            }
            KafkaSystem.logger.debug((Object)"Tryin to Stop All Receivers");
        }
        synchronized (this.HostToReceiverInfo) {
            this.HostToReceiverInfo.remove(this.serverID);
            this.receiversInThisSystem.clear();
        }
    }
    
    @Override
    public boolean cleanupAllReceivers() {
        boolean isAlive = true;
        for (final String name : this.receiversInThisSystem.keySet()) {
            try {
                isAlive = this.killReceiver(name);
            }
            catch (Exception e) {
                KafkaSystem.logger.error((Object)e.getMessage());
            }
        }
        return isAlive;
    }
    
    public Map.Entry<UUID, ReceiverInfo> searchStreamName(final String streamName) {
        synchronized (this.HostToReceiverInfo) {
            final Set<UUID> peerUUID = this.HostToReceiverInfo.keySet();
            for (final UUID peer : peerUUID) {
                final List<ReceiverInfo> receiverList = this.HostToReceiverInfo.get(peer);
                for (final ReceiverInfo receiverInfo : receiverList) {
                    if (receiverInfo.getName().startsWith(streamName)) {
                        return new Map.Entry<UUID, ReceiverInfo>() {
                            @Override
                            public ReceiverInfo setValue(final ReceiverInfo value) {
                                return null;
                            }
                            
                            @Override
                            public ReceiverInfo getValue() {
                                return receiverInfo;
                            }
                            
                            @Override
                            public UUID getKey() {
                                return peer;
                            }
                        };
                    }
                }
            }
            return null;
        }
    }
    
    @Override
    public Class getReceiverClass() {
        return KafkaReceiver.class;
    }
    
    public Position getComponentCheckpoint() {
        final PathManager result = new PathManager();
        for (final KafkaReceiver r : this.receiversInThisSystem.values()) {
            result.mergeHigherPositions(r.getComponentCheckpoint());
        }
        return result.toPosition();
    }
    
    static {
        KafkaSystem.logger = Logger.getLogger((Class)KafkaSystem.class);
    }
}
