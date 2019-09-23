package com.datasphere.jmqmessaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jctools.queues.*;
import org.zeromq.ZContext;

import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.messaging.SocketType;
import com.datasphere.messaging.TransportMechanism;
import com.datasphere.uuid.UUID;

public abstract class AsyncSender extends ZMQSender
{
    private static final Logger logger;
    private static final long QUEUE_OPERATION_WAIT_MS = 10L;
    private static int QUEUE_SIZE_MAX;
    private static final int MAX_MESSAGE_SEND;
    private static final boolean DO_POLL;
    private static final boolean DO_OLD_PUBLISH;
    private static final int TIME_OUT;
    private final AsyncSenderThread senderThread;
    private final BlockingQueue<Object> messageQueue;
    private final List<Object> drainToMailbox;
    
    public AsyncSender(final ZContext ctx, final UUID serverID, final ZMQReceiverInfo info, final SocketType type, final TransportMechanism transportMechanism, final boolean isEncrypted) {
        super(ctx, serverID, (ReceiverInfo)info, type, transportMechanism, isEncrypted);
        this.drainToMailbox = new ArrayList<Object>(AsyncSender.MAX_MESSAGE_SEND + 1);
        try {
            AsyncSender.QUEUE_SIZE_MAX = Integer.parseInt(System.getProperty("com.datasphere.optimalBackPressureThreshold", "10000"));
        }
        catch (Exception e) {
            AsyncSender.logger.warn((Object)"Invalid setting for com.datasphere.optimalBackPressureThreshold, defaulting to 10000");
            AsyncSender.QUEUE_SIZE_MAX = 10000;
        }
        (this.senderThread = new AsyncSenderThread()).setName(this.getInfo().getName() + ":Async-Sender");
        this.messageQueue = new MpscCompoundQueue<Object>(AsyncSender.QUEUE_SIZE_MAX);
        AsyncSender.logger.debug((Object)("Connection created for " + this.getInfo().getName() + " on " + serverID));
    }
    
    @Override
    public void start() throws InterruptedException {
        AsyncSender.logger.info((Object)("Starting Tcp/Inproc async Sender thread for sender " + this.getInfo().getName()));
        super.start();
        this.messageQueue.clear();
        this.senderThread.start();
        this.reportSenderThreadStatus();
    }
    
    @Override
    public void stop() {
        AsyncSender.logger.info((Object)("Stopping Tcp/Inproc async Sender thread for sender " + this.senderThread.getName()));
        this.senderThread.running = false;
        this.cleanup();
        try {
            this.senderThread.interrupt();
            this.senderThread.join(150L);
        }
        catch (InterruptedException ignored) {
            AsyncSender.logger.warn((Object)(Thread.currentThread().getName() + " interrupted " + this.senderThread.getName() + " as part of STOP routine, hence exception can be ignored."));
        }
        this.messageQueue.clear();
        this.reportSenderThreadStatus();
    }
    
    protected abstract void cleanup();
    
    private void reportSenderThreadStatus() {
        AsyncSender.logger.debug((Object)("Status of Sender Thread (" + this.senderThread.getName() + "): " + AsyncSender.QUEUE_SIZE_MAX + ", " + this.senderThread.running));
    }
    
    protected abstract void destroySocket();
    
    protected abstract void processMessage(final Object p0);
    
    @Override
    public boolean send(final Object message) throws InterruptedException {
        for (boolean sent = false; !sent && this.senderThread.running; sent = this.messageQueue.offer(message, 100L, TimeUnit.MILLISECONDS)) {}
        return this.senderThread.running;
    }
    
    @Override
    public boolean isFull() {
        final boolean result = this.messageQueue.size() >= AsyncSender.QUEUE_SIZE_MAX - 2;
        return result;
    }
    
    static {
        logger = Logger.getLogger((Class)AsyncSender.class);
        AsyncSender.QUEUE_SIZE_MAX = 10000;
        MAX_MESSAGE_SEND = Integer.parseInt(System.getProperty("com.datasphere.async.ASYNC_THRESHOLD", "50"));
        DO_POLL = Boolean.parseBoolean(System.getProperty("com.datasphere.async.POLL", "true"));
        DO_OLD_PUBLISH = Boolean.parseBoolean(System.getProperty("com.datasphere.async.SIMPLE", "false"));
        TIME_OUT = Integer.parseInt(System.getProperty("com.datasphere.async.TIME_OUT", "50"));
    }
    
    private class AsyncSenderThread extends Thread
    {
        private volatile boolean running;
        
        private AsyncSenderThread() {
            this.running = true;
        }
        
        @Override
        public void run() {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (AsyncSender.DO_OLD_PUBLISH) {
                        try {
                            final Object message = AsyncSender.this.messageQueue.poll(100L, TimeUnit.MILLISECONDS);
                            if (message == null) {
                                continue;
                            }
                            AsyncSender.this.processMessage(message);
                        }
                        catch (Exception e) {
                            AsyncSender.logger.error((Object)"Error publishing message", (Throwable)e);
                        }
                    }
                    else if (AsyncSender.DO_POLL) {
                        AsyncSender.this.drainToMailbox.clear();
                        final Object firstMessage = AsyncSender.this.messageQueue.poll(AsyncSender.TIME_OUT, TimeUnit.MILLISECONDS);
                        if (firstMessage == null) {
                            continue;
                        }
                        AsyncSender.this.drainToMailbox.add(firstMessage);
                        AsyncSender.this.messageQueue.drainTo(AsyncSender.this.drainToMailbox, AsyncSender.MAX_MESSAGE_SEND);
                        for (final Object mail : AsyncSender.this.drainToMailbox) {
                            AsyncSender.this.processMessage(mail);
                        }
                    }
                    else {
                        AsyncSender.this.drainToMailbox.clear();
                        AsyncSender.this.messageQueue.drainTo(AsyncSender.this.drainToMailbox, AsyncSender.MAX_MESSAGE_SEND);
                        for (final Object mail2 : AsyncSender.this.drainToMailbox) {
                            AsyncSender.this.processMessage(mail2);
                        }
                        if (AsyncSender.this.drainToMailbox.size() != 0) {
                            continue;
                        }
                        final Object firstMessage = AsyncSender.this.messageQueue.poll(AsyncSender.TIME_OUT, TimeUnit.MILLISECONDS);
                        AsyncSender.this.processMessage(firstMessage);
                    }
                }
                catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        if (!this.running) {
                            AsyncSender.logger.info((Object)(this.getName() + " interrupted but component is in STOP rountine, hence exception ignored."), (Throwable)e);
                        }
                        else {
                            AsyncSender.logger.error((Object)(this.getName() + " interrupted and component not in STOP rountine."), (Throwable)e);
                        }
                        break;
                    }
                    AsyncSender.logger.error((Object)"Error publishing message", (Throwable)e);
                }
            }
            if (Thread.currentThread().isInterrupted()) {
                if (!this.running) {
                    AsyncSender.logger.info((Object)(this.getName() + " interrupted but component is in STOP rountine, hence exception ignored."));
                }
                else {
                    AsyncSender.logger.error((Object)(this.getName() + " interrupted and component not in STOP rountine."));
                }
            }
            AsyncSender.this.destroySocket();
            AsyncSender.logger.info((Object)("Exiting Tcp/Inproc async Sender thread for sender " + AsyncSender.this.senderThread.getName()));
        }
    }
}
