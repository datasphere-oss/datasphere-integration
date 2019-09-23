package com.datasphere.runtime.channels;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.distribution.HQueue;
import com.datasphere.exceptionhandling.HExceptionMgr;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.AdhocStreamSubscriber;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorModel;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

import scala.NotImplementedError;

public abstract class DistributedChannel implements Channel
{
    public MetaInfo.Stream stream_metainfo;
    public MessagingSystem messagingSystem;
    protected BaseServer srv;
    protected boolean encrypted;
    protected Stream owner;
    protected Status status;
    UUID thisPeerId;
    private static Logger logger;
    protected KeyFactory keyFactory;
    
    public DistributedChannel(final Stream owner) {
        this.encrypted = false;
        this.status = Status.INITIALIZED;
        this.thisPeerId = HazelcastSingleton.getNodeId();
        this.owner = owner;
        this.stream_metainfo = owner.getStreamMeta();
        try {
            this.keyFactory = KeyFactory.createKeyFactory(this.stream_metainfo);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    protected MetaInfo.Stream getStreamInfo() {
        return this.owner.getStreamMeta();
    }
    
    public abstract boolean isFull();
    
    public Position getCheckpoint() {
        return null;
    }
    
    boolean checkIfSubscriberAndStreamBelongToSameApp(final UUID subscriberID) {
        if (!(this.srv instanceof Server)) {
            return false;
        }
        final Flow owningApplication = this.getOwner().getTopLevelFlow();
        boolean isPresent = false;
        if (owningApplication != null) {
            final MetaInfo.Flow owningApplicationMetaObject = owningApplication.getFlowMeta();
            try {
                isPresent = owningApplicationMetaObject.getAllObjects().contains(subscriberID);
            }
            catch (MetaDataRepositoryException e) {
                DistributedChannel.logger.warn((Object)e.getMessage());
            }
        }
        return isPresent;
    }
    
    public String getName() {
        return this.owner.getMetaName();
    }
    
    public UUID getID() {
        return this.owner.getMetaID();
    }
    
    public abstract void stop();
    
    public abstract void start();
    
    public abstract void start(final Map<UUID, Set<UUID>> p0);
    
    public abstract boolean verifyStart(final Map<UUID, Set<UUID>> p0);
    
    public abstract void closeDistSub(final String p0);
    
    public abstract Set<DistLink> getSubscribers();
    
    public String getDebugId() {
        try {
            return id2name(this.thisPeerId) + "-" + this.stream_metainfo.getName();
        }
        catch (MetaDataRepositoryException e) {
            DistributedChannel.logger.error((Object)e.getMessage());
            return null;
        }
    }
    
    static String id2name(final UUID id) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, HSecurityManager.TOKEN);
        if (o instanceof MetaInfo.Server) {
            final MetaInfo.Server s = (MetaInfo.Server)o;
            return "srv_" + s.id;
        }
        return id.toString();
    }
    
    public MetaInfo.Stream getMetaObject() {
        return this.stream_metainfo;
    }
    
    public Stream getOwner() {
        return this.owner;
    }
    
    UUID getSubscriberID(final Subscriber sub) {
        if (sub instanceof FlowComponent) {
            final FlowComponent so = (FlowComponent)sub;
            return so.getMetaID();
        }
        if (sub instanceof MonitorModel) {
            return MonitorModel.getMonitorModelID();
        }
        if (sub instanceof HQueue.ShowStreamSubscriber) {
            return ((HQueue.ShowStreamSubscriber)sub).uuid;
        }
        if (sub instanceof AdhocStreamSubscriber) {
            return ((AdhocStreamSubscriber)sub).getUuid();
        }
        if (sub instanceof HExceptionMgr) {
            return HExceptionMgr.ExceptionStreamUUID;
        }
        return null;
    }
    
    String getSubscriberName(final Subscriber sub) {
        if (sub instanceof FlowComponent) {
            final FlowComponent so = (FlowComponent)sub;
            return so.getMetaName();
        }
        if (sub instanceof MonitorModel) {
            return "InternalMonitoring";
        }
        if (sub instanceof HQueue.ShowStreamSubscriber) {
            return ((HQueue.ShowStreamSubscriber)sub).name;
        }
        if (sub instanceof HExceptionMgr) {
            return ((HExceptionMgr)sub).getName();
        }
        if (sub instanceof AdhocStreamSubscriber) {
            return ((AdhocStreamSubscriber)sub).getName();
        }
        return null;
    }
    
    public static String buildReceiverName(final MetaInfo.Stream stream_metainfo, final UUID subID) {
        final String receiver_name_builder = stream_metainfo.getNsName() + ":" + stream_metainfo.getName() + ":" + stream_metainfo.getUuid() + ":" + subID;
        return receiver_name_builder;
    }
    
    public void startEmitting(final SourcePosition position) throws Exception {
        throw new NotImplementedError();
    }
    
    static {
        DistributedChannel.logger = Logger.getLogger((Class)DistributedChannel.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        RUNNING;
    }
}
