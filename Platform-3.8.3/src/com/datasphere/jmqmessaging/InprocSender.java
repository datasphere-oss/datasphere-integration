package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import org.zeromq.*;
import com.datasphere.uuid.*;
import com.datasphere.messaging.*;

public class InprocSender extends ZMQSender
{
    private static Logger logger;
    private final ZMQReceiver msgr;
    
    public InprocSender(final ZContext ctx, final UUID serverID, final ZMQReceiverInfo info, final SocketType type, final ZMQReceiver msgr) {
        super(ctx, serverID, (ReceiverInfo)info, type, TransportMechanism.INPROC, false);
        this.msgr = msgr;
        if (InprocSender.logger.isDebugEnabled()) {
            InprocSender.logger.debug((Object)("Connection created for " + info.getName() + " on " + serverID));
        }
    }
    
    @Override
    public void start() throws InterruptedException {
    }
    
    @Override
    public void stop() {
    }
    
    @Override
    public boolean send(final Object data) {
        this.msgr.getRcvr().onMessage(data);
        return true;
    }
    
    @Override
    public boolean isFull() {
        return false;
    }
    
    static {
        InprocSender.logger = Logger.getLogger((Class)InprocSender.class);
    }
}
