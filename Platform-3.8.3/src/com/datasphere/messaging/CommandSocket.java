package com.datasphere.messaging;

import org.apache.log4j.*;
import org.zeromq.*;

public class CommandSocket
{
    private static Logger logger;
    public static final byte STOP = 1;
    private final ZContext context;
    private String endpoint;
    
    public CommandSocket() {
        this.context = null;
    }
    
    public CommandSocket(final ZContext context) {
        this.context = context;
    }
    
    public void open(final String endpoint) {
        this.endpoint = endpoint;
        if (CommandSocket.logger.isDebugEnabled()) {
            CommandSocket.logger.debug((Object)("Command socket for " + endpoint + " ready"));
        }
    }
    
    public void sendCommand(final byte cmd) {
        final byte[] msg = { cmd };
        final ZMQ.Socket peer = this.context.createSocket(8);
        if (this.endpoint != null) {
            peer.connect(this.endpoint);
            peer.setDelayAttachOnConnect(true);
            peer.send(msg, 0);
            if (CommandSocket.logger.isDebugEnabled()) {
                CommandSocket.logger.debug((Object)("Sent control message to " + this.endpoint));
            }
        }
        else {
            CommandSocket.logger.error((Object)("Can't close socket at: " + this.endpoint));
        }
        this.context.destroySocket(peer);
    }
    
    static {
        CommandSocket.logger = Logger.getLogger((Class)CommandSocket.class);
    }
}
