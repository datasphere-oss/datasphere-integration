package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.messaging.*;
import java.util.*;
import org.zeromq.*;

public class PullReceiver extends ZMQReceiver
{
    private static Logger logger;
    protected ZMQ.Socket receiverSocket;
    private final int HWM = 50000;
    private final int bufferSize = 1048576;
    private final int linger = 0;
    private Thread puller;
    private CommandSocket command;
    
    public PullReceiver(final ZContext ctx, final Handler rcvr, final String name, final boolean encrypted) {
        super(ctx, rcvr, name, encrypted);
        this.command = new CommandSocket(ctx);
        (this.puller = new Thread(new ZMQPuller(this, rcvr, this.command))).setName(name + "-rcvr");
    }
    
    @Override
    public void start(final Map<Object, Object> properties) throws Exception {
        if (this.receiverSocket != null) {
            return;
        }
        (this.receiverSocket = this.getCtx().createSocket(7)).setRcvHWM(50000L);
        this.receiverSocket.setLinger(0L);
        this.receiverSocket.setReceiveBufferSize(1048576L);
        try {
            final int tcp = this.receiverSocket.bind(this.getTcpAddress());
            if (tcp == -1) {
                throw new Exception("Did not bind to appropriate TCP Address");
            }
            super.setTcpPort(tcp);
        }
        catch (ZMQException e) {
            PullReceiver.logger.error((Object)(this.getName() + " was trying to bind to: " + this.getTcpAddress() + " but, failed"), (Throwable)e);
            throw e;
        }
        try {
            final int ipc = this.receiverSocket.bind(this.getIpcAddress());
            if (ipc == -1) {
                throw new Exception(this.getIpcAddress() + " is already in use!");
            }
            super.setIpcPort(ipc);
        }
        catch (ZMQException e) {
            PullReceiver.logger.error((Object)(this.getName() + " was trying to bind to: " + this.getIpcAddress() + " but, failed"), (Throwable)e);
            throw e;
        }
        try {
            final int inproc = this.receiverSocket.bind(this.getInProcAddress());
            if (inproc == -1) {
                throw new Exception(this.getInProcAddress() + " is already is use!");
            }
            this.makeAddress();
            if (this.getInProcAddress() != null) {
                this.command.open(this.getInProcAddress());
            }
            else if (PullReceiver.logger.isDebugEnabled()) {
                PullReceiver.logger.debug((Object)("In Proc Address can't be NULL for: " + this.getName()));
            }
            this.puller.start();
        }
        catch (ZMQException e) {
            PullReceiver.logger.error((Object)(this.getName() + " was trying to bind to: " + this.getInProcAddress() + " but, failed"), (Throwable)e);
            throw e;
        }
    }
    
    @Override
    public boolean stop() throws Exception {
        PullReceiver.logger.info((Object)("Stopping " + this.getClass().getSimpleName() + "  : " + this.getName()));
        if (this.puller.isAlive()) {
            this.command.sendCommand((byte)1);
        }
        while (this.puller.isAlive()) {
            this.puller.join(500L);
            if (PullReceiver.logger.isDebugEnabled()) {
                PullReceiver.logger.debug((Object)("Receiver : " + this.getName() + " is still alive!"));
            }
        }
        if (PullReceiver.logger.isDebugEnabled()) {
            PullReceiver.logger.debug((Object)("Receiver : " + this.getName() + " isAlive? " + this.puller.isAlive()));
        }
        this.receiverSocket = null;
        return this.puller.isAlive();
    }
    
    static {
        PullReceiver.logger = Logger.getLogger((Class)PullReceiver.class);
    }
}
