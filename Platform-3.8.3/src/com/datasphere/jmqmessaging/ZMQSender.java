package com.datasphere.jmqmessaging;

import java.util.Random;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.messaging.Sender;
import com.datasphere.messaging.SocketType;
import com.datasphere.messaging.TransportMechanism;
import com.datasphere.uuid.UUID;

import zmq.ZError;

public abstract class ZMQSender extends Sender
{
    private static final Logger logger;
    final ZContext ctx;
    final ZContext shadowCtx;
    protected final boolean isEncrypted;
    private ZMQ.Socket socket;
    private final int sendBuffer = 1048576;
    private final int hwm = 50000;
    private final int linger = 0;
    private final int sendTimeOut = -1;
    private boolean hasStarted;
    private final SocketType type;
    private final Random rand;
    private String identity;
    
    public ZMQSender(final ZContext ctx, final UUID serverID, final ReceiverInfo info, final SocketType type, final TransportMechanism mechansim, final boolean isEncrypted) {
        super(serverID, info, mechansim);
        this.socket = null;
        this.hasStarted = false;
        this.rand = new Random(System.currentTimeMillis());
        this.identity = null;
        this.isEncrypted = isEncrypted;
        this.ctx = ctx;
        this.shadowCtx = ZContext.shadow(this.ctx);
        this.type = type;
    }
    
    public ZMQ.Socket getSocket() {
        return this.socket;
    }
    
    public SocketType getType() {
        return this.type;
    }
    
    @Override
    public void start() throws InterruptedException {
        try {
            if (this.hasStarted) {
                return;
            }
            if (this.getMechansim() == TransportMechanism.INPROC) {
                return;
            }
            switch (this.type) {
                case PUB: {
                    this.socket = this.shadowCtx.createSocket(1);
                    break;
                }
                case SUB: {
                    this.socket = this.shadowCtx.createSocket(2);
                    break;
                }
                case SYNCREQ: {
                    this.socket = this.shadowCtx.createSocket(3);
                    break;
                }
                case ASYNCREQ: {
                    this.socket = this.shadowCtx.createSocket(5);
                    break;
                }
                case SYNCREP: {
                    this.socket = this.shadowCtx.createSocket(4);
                    break;
                }
                case ASYNCREP: {
                    this.socket = this.shadowCtx.createSocket(6);
                    break;
                }
                case PUSH: {
                    this.socket = this.shadowCtx.createSocket(8);
                    break;
                }
                case PULL: {
                    this.socket = this.shadowCtx.createSocket(7);
                    break;
                }
            }
            this.setIdentity(this.socket);
            this.socket.setLinger(0L);
            this.socket.setSendBufferSize(1048576L);
            this.socket.setHWM(50000L);
            this.socket.setDelayAttachOnConnect(false);
            this.socket.setSendTimeOut(-1);
            switch (this.getMechansim()) {
                case INPROC: {
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Trying to connect to : " + this.getInfo().getName() + " at " + this.getInfo().getInprocURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Trying to connect to : " + this.getInfo().getName()));
                    }
                    this.socket.connect(this.getInfo().getInprocURI());
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Connected to : " + this.getInfo().getName() + " at " + this.getInfo().getInprocURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Connected to : " + this.getInfo().getName()));
                        break;
                    }
                    break;
                }
                case IPC: {
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Trying to connect to : " + this.getInfo().getName() + " at " + this.getInfo().getIpcURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Trying to connect to : " + this.getInfo().getName()));
                    }
                    this.socket.connect(this.getInfo().getIpcURI());
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Connected to : " + this.getInfo().getName() + " at " + this.getInfo().getIpcURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Connected to : " + this.getInfo().getName()));
                        break;
                    }
                    break;
                }
                case TCP: {
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Trying to connect to : " + this.getInfo().getName() + " at " + this.getInfo().getTcpURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Trying to connect to : " + this.getInfo().getName()));
                    }
                    this.socket.connect(this.getInfo().getTcpURI());
                    if (ZMQSender.logger.isDebugEnabled()) {
                        ZMQSender.logger.debug((Object)("Connected to : " + this.getInfo().getName() + " at " + this.getInfo().getTcpURI()));
                    }
                    if (ZMQSender.logger.isInfoEnabled()) {
                        ZMQSender.logger.info((Object)("Connected to : " + this.getInfo().getName()));
                        break;
                    }
                    break;
                }
            }
            this.hasStarted = true;
        }
        catch (Throwable e) {
            if (e instanceof ZError.IOException) {
                throw new InterruptedException(e.getMessage());
            }
            ZMQSender.logger.error((Object)("Problem starting ZMQReceiver " + this.getInfo()), e);
        }
    }
    
    public void setIdentity(final ZMQ.Socket sock) {
        this.identity = String.format("%08X-%08X", this.rand.nextInt(), this.rand.nextInt());
        sock.setIdentity(this.identity.getBytes(ZMQ.CHARSET));
    }
    
    public String getIdentity() {
        return this.identity;
    }
    
    @Override
    public void stop() {
        this.ctx.destroySocket(this.socket);
        this.shadowCtx.destroy();
    }
    
    @Override
    public abstract boolean send(final Object p0) throws InterruptedException;
    
    static {
        logger = Logger.getLogger((Class)ZMQSender.class);
    }
}
