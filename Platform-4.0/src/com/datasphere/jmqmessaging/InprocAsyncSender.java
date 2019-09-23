package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.exception.*;
import com.datasphere.messaging.*;
import java.util.concurrent.*;

import zmq.*;
import java.nio.channels.*;
import org.zeromq.*;

public class InprocAsyncSender extends AsyncSender
{
    private static final Logger logger;
    private final Handler receiveHandler;
    
    public InprocAsyncSender(final ZContext ctx, final UUID serverID, final ZMQReceiverInfo info, final SocketType type, final ZMQReceiver receiver) {
        super(ctx, serverID, info, type, TransportMechanism.INPROC, false);
        this.receiveHandler = receiver.getRcvr();
    }
    
    @Override
    protected void cleanup() {
    }
    
    @Override
    protected void destroySocket() {
    }
    
    @Override
    protected void processMessage(final Object data) {
        try {
            this.receiveHandler.onMessage(data);
        }
        catch (RejectedExecutionException e) {
            InprocAsyncSender.logger.warn((Object)("got exception for async sender thread " + e.getMessage()));
        }
        catch (RuntimeInterruptedException e2) {
            InprocAsyncSender.logger.warn((Object)("Got RuntimeInterruptedException " + e2.getMessage()));
        }
        catch (ZError.IOException | ClosedSelectorException | ZMQException ex2) {
            InprocAsyncSender.logger.warn((Object)("Got ZMQException " + ex2.getMessage()));
        }
        catch (com.hazelcast.core.RuntimeInterruptedException e4) {
            InprocAsyncSender.logger.warn((Object)("Got RuntimeInterruptedException " + e4.getMessage()));
        }
        catch (RuntimeException e3) {
            InprocAsyncSender.logger.warn((Object)("got exception for async sender thread " + e3.getMessage()));
        }
        catch (Exception e5) {
            InprocAsyncSender.logger.warn((Object)("got exception for async sender thread " + e5.getMessage()));
        }
    }
    
    static {
        logger = Logger.getLogger((Class)InprocAsyncSender.class);
    }
}
