package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.messaging.*;
import com.datasphere.ser.*;
import java.nio.channels.*;
import zmq.*;

import com.datasphere.exception.*;
import com.esotericsoftware.kryo.*;

import org.zeromq.*;

public class ZMQPuller implements Runnable
{
    private static Logger logger;
    private PullReceiver pullReceiver;
    private ZMsg msg;
    private final Handler rcvr;
    public CommandSocket command;
    
    public ZMQPuller(final PullReceiver pullReceiver, final Handler rcvr, final CommandSocket command) {
        this.msg = null;
        this.pullReceiver = pullReceiver;
        this.rcvr = rcvr;
        this.command = command;
    }
    
    @Override
    public void run() {
        if (ZMQPuller.logger.isInfoEnabled()) {
            ZMQPuller.logger.info((Object)(this.pullReceiver.getName() + " ready to receive data"));
        }
        if (ZMQPuller.logger.isDebugEnabled()) {
            ZMQPuller.logger.debug((Object)(this.pullReceiver.getName() + " ready to receive msgs at : \n 1. " + this.pullReceiver.getTcpAddress() + "\n 2. " + this.pullReceiver.getIpcAddress() + " \n 3. " + this.pullReceiver.getInProcAddress() + "\n-------------------------------------------------------------------"));
        }
        try {
            while (!Thread.interrupted()) {
                try {
                    this.msg = ZMsg.recvMsg(this.pullReceiver.receiverSocket);
                    Label_0233: {
                        if (this.msg != null) {
                            break Label_0233;
                        }
                        if (Thread.interrupted()) {
                            ZMQPuller.logger.info((Object)("Got a null message in " + this.pullReceiver.getName() + " as part of an interrupt signal, safe to move on."));
                            break;
                        }
                        try {
                            ZMQPuller.logger.warn((Object)("Got a null message in " + this.pullReceiver.getName() + ", continue receiving more messages."));
                            final ZFrame frame = this.msg.pop();
                            final byte[] bytes = frame.getData();
                            Label_0337: {
                                if (bytes == null) {
                                    break Label_0337;
                                }
                                if (bytes.length == 1) {
                                    if (bytes[0] == 1) {
                                        if (ZMQPuller.logger.isDebugEnabled()) {
                                            ZMQPuller.logger.debug((Object)("Received control message at(" + this.pullReceiver.getName() + ") : " + bytes[0]));
                                        }
                                        break;
                                    }
                                    break Label_0337;
                                }
                                try {
                                    this.rcvr.onMessage(KryoSingleton.read(bytes, this.pullReceiver.isEncrypted));
                                    frame.destroy();
                                    continue;
                                }
                                catch (ClosedSelectorException e) {
                                    ZMQPuller.logger.debug((Object)("Got ZMQException " + e.getMessage()));
                                    continue;
                                }
                            }
                        }
                        catch (ClosedSelectorException ex2) {}
                    }
                }
                catch (ZError.IOException ex3) {}
                catch (ClosedSelectorException ex4) {}
                catch (ZMQException ex5) {}
                catch (KryoException e2) {
                    if (e2.getMessage().contains("Buffer underflow")) {
                        ZMQPuller.logger.debug((Object)("Got Kryoexception " + e2.getMessage()));
                        continue;
                    }
                    throw e2;
                }
                catch (RuntimeInterruptedException e3) {
                    ZMQPuller.logger.warn((Object)("Got RuntimeInterruptedException " + e3.getMessage()));
                }
                catch (com.hazelcast.core.RuntimeInterruptedException e4) {
                    ZMQPuller.logger.warn((Object)("Got RuntimeInterruptedException " + e4.getMessage()));
                }
                break;
            }
        }
        finally {
            ZMQPuller.logger.info((Object)("Receiver " + this.pullReceiver.getName() + " stopped!"));
            try {
                this.pullReceiver.getCtx().destroySocket(this.pullReceiver.receiverSocket);
                if (ZMQPuller.logger.isInfoEnabled()) {
                    ZMQPuller.logger.info((Object)("Destroyed socket associated with " + this.pullReceiver.getName()));
                }
            }
            catch (ZError.IOException | ClosedSelectorException | ZMQException ex6) {
                ZMQPuller.logger.warn((Object)("Exception while destroying socket for " + this.pullReceiver.getName() + ", exception message: " + ex6.getMessage()));
            }
        }
    }
    
    static {
        ZMQPuller.logger = Logger.getLogger((Class)ZMQPuller.class);
    }
}
