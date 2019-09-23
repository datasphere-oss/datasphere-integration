package com.datasphere.runtime;

import org.apache.log4j.*;
import com.datasphere.runtime.channels.*;
import com.datasphere.recovery.*;

public abstract class KafkaDistSub extends DistSub
{
    protected static Logger logger;
    protected final String streamSubscriberName;
    public DistLink distLink;
    protected BaseServer srvr;
    protected DistributedChannel parentStream;
    protected final boolean encrypted;
    protected ConsistentHashRing ring;
    protected KafkaDistributedRcvr distributedRcvr;
    public Status status;
    
    public KafkaDistSub(final DistLink l, final DistributedChannel pStream, final BaseServer srvr, final boolean encrypted) {
        this.distLink = l;
        this.srvr = srvr;
        this.parentStream = pStream;
        this.encrypted = encrypted;
        this.streamSubscriberName = pStream.getMetaObject().getName() + "-" + this.distLink.getName();
        this.status = Status.INITIALIZED;
    }
    
    @Override
    public void stop() {
        if (this.status != Status.STARTED) {
            if (KafkaDistSub.logger.isDebugEnabled() && KafkaDistSub.logger.isDebugEnabled()) {
                KafkaDistSub.logger.debug((Object)("Stream subscriber " + this.streamSubscriberName + " is not started so not stopping"));
            }
            return;
        }
        if (this.distributedRcvr != null) {
            this.distributedRcvr.stop();
        }
        this.distributedRcvr = null;
        this.status = Status.STOPPED;
        if (KafkaDistSub.logger.isInfoEnabled()) {
            KafkaDistSub.logger.info((Object)("Stopped stream subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName()));
        }
        this.postStopHook();
    }
    
    public boolean isFull() {
        return false;
    }
    
    @Override
    public void sendEvent(final EventContainer event, final int sendQueueId, final LagMarker islagmarker) throws InterruptedException {
    }
    
    public void startEmitting(final PartitionedSourcePosition position) throws Exception {
        if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
            Logger.getLogger("KafkaStreams").debug((Object)("Starting KafkaDistSub " + this.streamSubscriberName + " at " + position));
        }
        if (this.status != Status.STARTED) {
            this.distributedRcvr.startEmitting(this.distributedRcvr.getName(), position);
            this.start();
        }
    }
    
    public Position getComponentCheckpoint() {
        return this.distributedRcvr.getComponentCheckpoint();
    }
    
    static {
        KafkaDistSub.logger = Logger.getLogger((Class)KafkaDistSub.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        STARTED, 
        STOPPED;
    }
}
