package com.datasphere.runtime;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.Sender;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Stream;
import com.datasphere.uuid.UUID;

import scala.NotImplementedError;

public abstract class DistSub implements ChannelEventHandler
{
    protected static Logger logger;
    public DistLink dist_link;
    private boolean receiveDataSynchronously;
    
    public DistSub() {
        this.receiveDataSynchronously = false;
    }
    
    protected abstract UUID getFirstPeer();
    
    public abstract void start();
    
    public abstract void start(final Map<UUID, Set<UUID>> p0);
    
    public abstract void startDistributedReceiver(final Map<UUID, Set<UUID>> p0) throws Exception;
    
    public abstract void stop();
    
    public abstract void postStopHook();
    
    public abstract void close(final UUID p0);
    
    public abstract boolean verifyStart(final Map<UUID, Set<UUID>> p0);
    
    public abstract void addPeer(final UUID p0);
    
    public abstract int getPeerCount();
    
    public abstract void addDistributedRcvr(final String p0, final Stream p1, final DistributedChannel p2, final boolean p3, final MessagingSystem p4, final Link p5, final Map<UUID, Set<UUID>> p6);
    
    public abstract Sender getSender(final int p0, final int p1) throws InterruptedException;
    
    public void startEmitting(final Position position) {
        throw new NotImplementedError();
    }
    
    public void setReceiveDataSynchronously(final boolean b) {
        this.receiveDataSynchronously = b;
    }
    
    public boolean canReceiveDataSynchronously() {
        return this.receiveDataSynchronously;
    }
    
    static {
        DistSub.logger = Logger.getLogger((Class)DistSub.class);
    }
    
    public enum Status
    {
        UNKNOWN, 
        INITIALIZED, 
        STARTED, 
        Status, 
        STOPPED;
    }
}
