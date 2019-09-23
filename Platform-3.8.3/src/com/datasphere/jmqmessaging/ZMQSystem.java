package com.datasphere.jmqmessaging;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;

import com.hazelcast.core.RuntimeInterruptedException;
import com.datasphere.messaging.Handler;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.NoDataConsumerException;
import com.datasphere.messaging.Receiver;
import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.messaging.Sender;
import com.datasphere.messaging.SocketType;
import com.datasphere.messaging.TransportMechanism;
import com.datasphere.messaging.UnknownObjectException;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.exceptions.NodeNotFoundException;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

import zmq.ZError;

public class ZMQSystem implements MessagingSystem
{
    private static Logger logger;
    private final String ipAddress;
    private final UUID serverID;
    private final Boolean isTcpAsyncDisabled;
    private final Map<UUID, List<ReceiverInfo>> HostToReceiverInfo;
    private Map<UUID, String> serverIdToMacId;
    private Map<String, ZMQReceiver> receiversInThisSystem;
    private final ZContext ctx;
    private List<UUID> sameHostPeers;
    private String macId;
    private final String IS_ASYNC_STRING;
    private final boolean IS_ASYNC;
    
    public ZMQSystem(final UUID serverID, final String ipAddress) {
        this.IS_ASYNC_STRING = System.getProperty("com.datasphere.config.asyncSend", "True");
        this.IS_ASYNC = this.IS_ASYNC_STRING.equalsIgnoreCase("True");
        this.serverID = serverID;
        this.ipAddress = ipAddress;
        this.HostToReceiverInfo = HazelcastSingleton.get().getMap("#HostToMessengerMap");
        this.serverIdToMacId = HazelcastSingleton.get().getMap("#serverIdToMacId");
        this.macId = HazelcastSingleton.getMacID();
        final String tcpAsync = System.getProperty("com.datasphere.config.disable-tcp-async", "false");
        this.isTcpAsyncDisabled = (tcpAsync != null && tcpAsync.equalsIgnoreCase("true"));
        synchronized (this.HostToReceiverInfo) {
            this.serverIdToMacId.put(this.serverID, this.macId);
            if (!this.HostToReceiverInfo.containsKey(this.serverID)) {
                this.HostToReceiverInfo.put(this.serverID, new ArrayList<ReceiverInfo>());
            }
        }
        this.receiversInThisSystem = new ConcurrentHashMap<String, ZMQReceiver>();
        this.sameHostPeers = new ArrayList<UUID>();
        this.ctx = new ZContext(1);
        if (ZMQSystem.logger.isInfoEnabled()) {
            ZMQSystem.logger.info((Object)("ZMQ System Initialized: ID=" + serverID + " Address=" + ipAddress + " MAC=" + this.macId + "\nZMQ Known MacIds: " + this.serverIdToMacId.entrySet()));
        }
    }
    
    @Override
    public void createReceiver(final Class clazz, final Handler handler, final String name, final boolean encrypted, final MetaInfo.Stream paramNotUsed) {
        if (clazz == null) {
            throw new NullPointerException("Can't create a Receiver out of a Null Class!");
        }
        if (name == null) {
            throw new NullPointerException("Name of a Receiver can't be Null!");
        }
        if (ZMQSystem.logger.isDebugEnabled()) {
            ZMQSystem.logger.debug((Object)("Creating receiver for " + name));
        }
        final Object[] constructorParams = { this.ctx, handler, name, encrypted };
        final Class[] classes = { ZContext.class, Handler.class, String.class, Boolean.TYPE };
        if (clazz.getSuperclass().equals(ZMQReceiver.class)) {
            try {
                final Constructor cc = clazz.getDeclaredConstructor((Class[])classes);
                final ZMQReceiver receiver = (ZMQReceiver)cc.newInstance(constructorParams);
                receiver.setIpAddress(this.ipAddress);
                receiver.setServerId(this.serverID);
                receiver.makeAddress();
                this.receiversInThisSystem.put(receiver.getName(), receiver);
            }
            catch (NoSuchMethodException e) {
                ZMQSystem.logger.error((Object)"No such method", (Throwable)e);
            }
            catch (InvocationTargetException e2) {
                ZMQSystem.logger.error((Object)"Failed invocation", (Throwable)e2);
            }
            catch (InstantiationException e3) {
                ZMQSystem.logger.error((Object)"Cannot instantiate", (Throwable)e3);
            }
            catch (IllegalAccessException e4) {
                ZMQSystem.logger.error((Object)"Access denied", (Throwable)e4);
            }
            return;
        }
        throw new UnknownObjectException("Can't create a Receiver for : " + clazz.getName() + ", expects it to be a Sub Class of a certain Platform class");
    }
    
    @Override
    public Receiver getReceiver(final String name) {
        return this.receiversInThisSystem.get(name);
    }
    
    @Override
    public void startReceiver(final String name, final Map<Object, Object> properties) throws Exception {
        final ZMQReceiver messenger = this.receiversInThisSystem.get(name);
        if (messenger != null) {
            try {
                messenger.start(properties);
            }
            catch (Exception e) {
                ZMQSystem.logger.error((Object)"Problem starting receiver", (Throwable)e);
                throw e;
            }
            this.updateReceiverMap(messenger);
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
        Sender sender;
        if (this.serverID.equals((Object)peerID)) {
            sender = this.getInProcInterfaceToReceiver(name, type, async);
        }
        else if (this.sameHostPeers.contains(peerID)) {
            sender = this.getIpcInterfaceToReceiver(peerID, name, type, isEncrypted);
        }
        else if (this.serverIdToMacId != null) {
            final String serverMacId = this.serverIdToMacId.get(this.serverID);
            final String peerMacId = this.serverIdToMacId.get(peerID);
            if (serverMacId != null && peerMacId != null && serverMacId.equals(peerMacId)) {
                if (!this.sameHostPeers.contains(peerID)) {
                    this.sameHostPeers.add(peerID);
                }
                sender = this.getIpcInterfaceToReceiver(peerID, name, type, isEncrypted);
            }
            else {
                sender = this.getTcpInterfaceToReceiver(peerID, name, type, isEncrypted);
            }
        }
        else {
            sender = this.getTcpInterfaceToReceiver(peerID, name, type, isEncrypted);
        }
        if (ZMQSystem.logger.isTraceEnabled()) {
            ZMQSystem.logger.trace((Object)("Sender for " + name + " on peer: " + peerID + " created"));
        }
        return sender;
    }
    
    public Sender getInProcInterfaceToReceiver(final String name, final SocketType type, final boolean async) throws NodeNotFoundException, InterruptedException {
        final ReceiverInfo rinfo = this.getReceiverInfo(this.serverID, name);
        if (rinfo == null) {
            throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
        }
        if (!(rinfo instanceof ZMQReceiverInfo)) {
            throw new NodeNotFoundException(name + " is not a ZMQReceiverInfo, it is a " + rinfo.getClass());
        }
        final ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
        final ZMQReceiver receiver = this.receiversInThisSystem.get(info.getName());
        final Sender sender = async ? new InprocAsyncSender(this.ctx, this.serverID, info, type, receiver) : new InprocSender(this.ctx, this.serverID, info, type, receiver);
        sender.start();
        return sender;
    }
    
    public Sender getIpcInterfaceToReceiver(final UUID peerID, final String name, final SocketType type, final boolean isEncrypted) throws NodeNotFoundException, InterruptedException {
        final ReceiverInfo rinfo = this.getReceiverInfo(peerID, name);
        if (rinfo == null) {
            throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
        }
        if (!(rinfo instanceof ZMQReceiverInfo)) {
            throw new NodeNotFoundException(name + " is not a ZMGReceiverInfo, it is a " + rinfo.getClass());
        }
        final ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
        if (ZMQSystem.logger.isDebugEnabled()) {
            ZMQSystem.logger.debug((Object)("receiver info for server " + peerID + "is :" + info));
        }
        final Sender ipc = ((boolean)this.isTcpAsyncDisabled) ? new TcpSender(this.ctx, this.serverID, info, type, TransportMechanism.IPC, isEncrypted) : new TcpAsyncSender(this.ctx, this.serverID, info, type, TransportMechanism.IPC, isEncrypted);
        ipc.start();
        return ipc;
    }
    
    public Sender getTcpInterfaceToReceiver(final UUID peerID, final String name, final SocketType type, final boolean isEncrypted) throws NodeNotFoundException, InterruptedException {
        final ReceiverInfo rinfo = this.getReceiverInfo(peerID, name);
        if (rinfo == null) {
            throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
        }
        if (!(rinfo instanceof ZMQReceiverInfo)) {
            throw new NodeNotFoundException(name + " is not a ZMGReceiverInfo, it is a " + rinfo.getClass());
        }
        final ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
        final Sender tcp = ((boolean)this.isTcpAsyncDisabled) ? new TcpSender(this.ctx, this.serverID, info, type, TransportMechanism.TCP, isEncrypted) : new TcpAsyncSender(this.ctx, this.serverID, info, type, TransportMechanism.TCP, isEncrypted);
        tcp.start();
        return tcp;
    }
    
    public void updateReceiverMap(final ZMQReceiver rcvr) {
        if (rcvr == null) {
            throw new NullPointerException("Receiver can't be null!");
        }
        synchronized (this.HostToReceiverInfo) {
            final List<ReceiverInfo> zmqReceiverInfoList = this.HostToReceiverInfo.get(this.serverID);
            final ZMQReceiverInfo info = new ZMQReceiverInfo(rcvr.getName(), this.ipAddress);
            if (this.validateCorrectness(zmqReceiverInfoList, info)) {
                if (ZMQSystem.logger.isDebugEnabled()) {
                    ZMQSystem.logger.debug((Object)("receiving info for " + this.serverID + " is :" + rcvr.getAddress()));
                }
                info.setAddress(rcvr.getAddress());
                zmqReceiverInfoList.add((ReceiverInfo)info);
                this.HostToReceiverInfo.put(this.serverID, zmqReceiverInfoList);
                if (ZMQSystem.logger.isTraceEnabled()) {
                    ZMQSystem.logger.trace((Object)("ZMQ Messengers on current(" + this.serverID + ") ZMQ System : \n" + this.receiversInThisSystem.keySet()));
                }
            }
            else if (ZMQSystem.logger.isDebugEnabled()) {
                ZMQSystem.logger.debug((Object)("Receiver " + rcvr.getName() + " already exists"));
            }
        }
    }
    
    public boolean validateCorrectness(final List<ReceiverInfo> zmqReceiverInfoList, final ZMQReceiverInfo info) {
        if (info == null) {
            throw new NullPointerException("Can't validate correctness of a Null Object!");
        }
        if (zmqReceiverInfoList.isEmpty()) {
            return true;
        }
        for (final ReceiverInfo mInfo : zmqReceiverInfoList) {
            if (mInfo.equals(info)) {
                return false;
            }
        }
        return true;
    }
    
    public ReceiverInfo getReceiverInfo(final UUID uuid, final String name) throws InterruptedException {
        ReceiverInfo info = null;
        int count = 0;
        while (info == null) {
            synchronized (this.HostToReceiverInfo) {
                List<ReceiverInfo> returnInfo = null;
                try {
                    returnInfo = this.HostToReceiverInfo.get(uuid);
                }
                catch (RuntimeInterruptedException e) {
                    throw new InterruptedException(e.getMessage());
                }
                if (returnInfo != null && !returnInfo.isEmpty()) {
                    for (final ReceiverInfo anInfo : returnInfo) {
                        final String name2 = anInfo.getName();
                        if (name.equals(name2)) {
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
            if (ZMQSystem.logger.isDebugEnabled()) {
                ZMQSystem.logger.debug((Object)("Stopped Receiver : " + name));
            }
            this.HostToReceiverInfo.put(this.serverID, rcvrs);
            this.receiversInThisSystem.remove(name);
        }
        if (ZMQSystem.logger.isTraceEnabled()) {
            ZMQSystem.logger.trace((Object)("HostToReceiver : " + this.HostToReceiverInfo.entrySet()));
            ZMQSystem.logger.trace((Object)("All receievers : " + this.receiversInThisSystem.entrySet()));
        }
        return isAlive;
    }
    
    public boolean killReceiver(final String name) throws Exception {
        final ZMQReceiver messenger = this.receiversInThisSystem.get(name);
        if (messenger != null) {
            boolean isAlive = messenger.stop();
            while (isAlive) {
                isAlive = messenger.stop();
                if (ZMQSystem.logger.isDebugEnabled()) {
                    ZMQSystem.logger.debug((Object)("Trying to stop receiver : " + name));
                }
                Thread.sleep(100L);
            }
            if (ZMQSystem.logger.isDebugEnabled()) {
                ZMQSystem.logger.debug((Object)("Stopped Receiver : " + name));
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
                ZMQSystem.logger.debug((Object)e.getMessage());
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
            catch (InterruptedException ex2) {}
            ZMQSystem.logger.debug((Object)"Tryin to Stop All Receivers");
        }
        synchronized (this.HostToReceiverInfo) {
            this.HostToReceiverInfo.remove(this.serverID);
            this.serverIdToMacId.remove(this.serverID);
            this.receiversInThisSystem.clear();
        }
        try {
            this.ctx.destroy();
        }
        catch (ZError.IOException | ClosedSelectorException | ZMQException ex3) {
            ZMQSystem.logger.warn(ex3.getMessage());
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
                ZMQSystem.logger.debug((Object)e.getMessage());
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
        return PullReceiver.class;
    }
    
    static {
        ZMQSystem.logger = Logger.getLogger((Class)ZMQSystem.class);
    }
}
