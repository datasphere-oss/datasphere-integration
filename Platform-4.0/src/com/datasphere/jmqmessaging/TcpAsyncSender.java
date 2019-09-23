package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.messaging.*;
import com.datasphere.ser.*;
import org.zeromq.*;

public class TcpAsyncSender extends AsyncSender
{
    private static Logger logger;
    
    public TcpAsyncSender(final ZContext ctx, final UUID serverID, final ZMQReceiverInfo info, final SocketType type, final TransportMechanism mechansim, final boolean isEncrypted) {
        super(new ZContext(1), serverID, info, type, mechansim, isEncrypted);
        if (TcpAsyncSender.logger.isDebugEnabled()) {
            TcpAsyncSender.logger.debug((Object)("TCP connection created for " + info.getName() + " on " + serverID));
        }
    }
    
    @Override
    protected void cleanup() {
        this.ctx.destroy();
    }
    
    @Override
    protected void destroySocket() {
        this.shadowCtx.destroySocket(this.getSocket());
    }
    
    @Override
    protected void processMessage(final Object data) {
        final ZMQ.Socket s = this.getSocket();
        final byte[] serializedData = KryoSingleton.write(data, this.isEncrypted);
        synchronized (s) {
            try {
                final boolean success = s.send(serializedData);
                if (!success) {
                    TcpAsyncSender.logger.warn((Object)"Failed to send message on stream");
                }
            }
            catch (ZMQException ex) {
                if (ex.getErrorCode() == 156384765) {
                    TcpAsyncSender.logger.info((Object)ex.getMessage());
                }
            }
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
        TcpAsyncSender.logger = Logger.getLogger((Class)TcpAsyncSender.class);
    }
}
