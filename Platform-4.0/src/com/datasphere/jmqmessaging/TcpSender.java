package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.messaging.*;
import com.datasphere.ser.*;
import org.zeromq.*;

public class TcpSender extends ZMQSender
{
    private static Logger logger;
    
    public TcpSender(final ZContext ctx, final UUID serverID, final ZMQReceiverInfo info, final SocketType type, final TransportMechanism mechansim, final boolean isEncrypted) {
        super(ctx, serverID, (ReceiverInfo)info, type, mechansim, isEncrypted);
        if (TcpSender.logger.isDebugEnabled()) {
            TcpSender.logger.debug((Object)("TCP connection created for " + info.getName() + " on " + serverID));
        }
    }
    
    @Override
    public boolean send(final Object data) {
        final ZMQ.Socket s = this.getSocket();
        final byte[] serializedData = KryoSingleton.write(data, this.isEncrypted);
        synchronized (s) {
            return s.send(serializedData);
        }
    }
    
    @Override
    public boolean isFull() {
        final ZMQ.Socket s = this.getSocket();
        final int events = s.getEvents();
        final boolean result = events == 0;
        return result;
    }
    
    static {
        TcpSender.logger = Logger.getLogger((Class)TcpSender.class);
    }
}
