package com.datasphere.jmqmessaging;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.util.concurrent.atomic.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.metaRepository.*;
import java.util.*;
import com.datasphere.messaging.*;
import com.datasphere.runtime.components.*;
import com.hazelcast.core.*;
import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

public class Executor<O> implements Handler
{
    private static Logger logger;
    String name;
    private transient MessagingSystem ms;
    private O on;
    UUID thisId;
    private From from;
    private static final String requestHandler = "-requestHandler";
    private static final String responseHandler = "-responseHandler";
    private Map<Long, ExecutableFuture<O, ?>> futures;
    private volatile boolean running;
    private ExecutorMembershipListener membershipListener;
    private String membershipListenerId;
    public static AtomicLong requestIdSeq;
    private Map<Class<?>, AtomicInteger> directExecution;
    private Map<Class<?>, AtomicInteger> sentRequests;
    private Map<Class<?>, AtomicInteger> sentResponses;
    private Map<Class<?>, AtomicInteger> rcvdRequests;
    private Map<Class<?>, AtomicInteger> rcvdResponses;
    Map<UUID, Sender> senderMapForRequests;
    Map<UUID, Sender> senderMapForResponses;
    
    public Executor(final String name, final O on) {
        this.futures = new ConcurrentHashMap<Long, ExecutableFuture<O, ?>>();
        this.directExecution = new ConcurrentHashMap<Class<?>, AtomicInteger>();
        this.sentRequests = new ConcurrentHashMap<Class<?>, AtomicInteger>();
        this.sentResponses = new ConcurrentHashMap<Class<?>, AtomicInteger>();
        this.rcvdRequests = new ConcurrentHashMap<Class<?>, AtomicInteger>();
        this.rcvdResponses = new ConcurrentHashMap<Class<?>, AtomicInteger>();
        this.senderMapForRequests = new ConcurrentHashMap<UUID, Sender>();
        this.senderMapForResponses = new ConcurrentHashMap<UUID, Sender>();
        this.name = "ExecutorService-" + name;
        this.ms = MessagingProvider.getMessagingSystem("com.datasphere.jmqmessaging.ZMQSystem");
        try {
            this.ms.createReceiver(PullReceiver.class, this, this.name.concat("-requestHandler"), false, null);
            this.ms.startReceiver(this.name.concat("-requestHandler"), new HashMap<Object, Object>());
            this.ms.createReceiver(PullReceiver.class, this, this.name.concat("-responseHandler"), false, null);
            this.ms.startReceiver(this.name.concat("-responseHandler"), new HashMap<Object, Object>());
        }
        catch (Exception e) {
            Executor.logger.error((Object)e.getMessage(), (Throwable)e);
        }
        this.on = on;
        this.thisId = HazelcastSingleton.getNodeId();
        this.from = new From(this.thisId, this.name);
        this.membershipListener = new ExecutorMembershipListener();
        this.membershipListenerId = HazelcastSingleton.get().getCluster().addMembershipListener((MembershipListener)this.membershipListener);
    }
    
    public void close() {
        try {
            this.ms.stopReceiver(this.name.concat("-requestHandler"));
            this.ms.stopReceiver(this.name.concat("-responseHandler"));
            this.stop();
            for (final Sender senders : this.senderMapForRequests.values()) {
                senders.stop();
            }
            for (final Sender senders : this.senderMapForResponses.values()) {
                senders.stop();
            }
            this.senderMapForRequests.clear();
            this.senderMapForResponses.clear();
            HazelcastSingleton.get().getCluster().removeMembershipListener(this.membershipListenerId);
        }
        catch (Exception ex) {}
    }
    
    public void start() {
        this.running = true;
    }
    
    public void stop() {
        final InterruptedException ie = new InterruptedException("Executor has been closed");
        for (final Map.Entry<Long, ExecutableFuture<O, ?>> futureEntry : this.futures.entrySet()) {
            futureEntry.getValue().setResponse(new ExecutionResponse<Object>(futureEntry.getKey(), ie));
        }
        this.running = false;
    }
    
    public UUID local() {
        return this.thisId;
    }
    
    public O on() {
        return this.on;
    }
    
    public <V> Future<V> execute(final UUID where, final Executable<O, V> executable) {
        final ExecutionRequest<O, V> request = new ExecutionRequest<O, V>(this.from, Executor.requestIdSeq.getAndIncrement(), executable);
        final ExecutableFuture<O, V> future = new ExecutableFuture<O, V>(request, where);
        if (this.thisId.equals((Object)where)) {
            future.setLocal(true);
            executable.setOn(this.on());
            ExecutionResponse<V> response = null;
            try {
                final V result = executable.call();
                if (executable.hasResponse()) {
                    response = new ExecutionResponse<V>(request.requestId, result);
                }
            }
            catch (Throwable t) {
                response = new ExecutionResponse<V>(request.requestId, t);
            }
            if (response != null) {
                future.setResponse(response);
            }
            this.updateStat(this.directExecution, request.executable.getClass());
        }
        else {
            future.setLocal(false);
            if (executable.hasResponse()) {
                if (!this.running) {
                    throw new IllegalStateException("Trying to make a blocking call in stopped state");
                }
                this.futures.put(request.requestId, future);
            }
            this.send(where, request, true);
            this.updateStat(this.sentRequests, request.executable.getClass());
        }
        return future;
    }
    
    private void updateStat(final Map<Class<?>, AtomicInteger> which, final Class<?> what) {
        AtomicInteger ai = which.get(what);
        if (ai == null) {
            ai = new AtomicInteger(1);
            which.put(what, ai);
        }
        else {
            ai.incrementAndGet();
        }
    }
    
    private int getStat(final Map<Class<?>, AtomicInteger> which, final Class<?> what) {
        final AtomicInteger ai = which.get(what);
        if (ai == null) {
            return 0;
        }
        return ai.get();
    }
    
    public void outputStats() {
        final Set<Class<?>> classes = new HashSet<Class<?>>();
        classes.addAll(this.directExecution.keySet());
        classes.addAll(this.sentRequests.keySet());
        classes.addAll(this.sentResponses.keySet());
        classes.addAll(this.rcvdRequests.keySet());
        classes.addAll(this.rcvdResponses.keySet());
        System.out.println("Overall stats:");
        for (final Class<?> c : classes) {
            System.out.println(c.getSimpleName() + " direct " + this.getStat(this.directExecution, c) + " sentreq " + this.getStat(this.sentRequests, c) + " sentrsp " + this.getStat(this.sentResponses, c) + " rcvdreq " + this.getStat(this.rcvdRequests, c) + " rcvdrsp " + this.getStat(this.rcvdResponses, c));
        }
    }
    
    private void send(final UUID where, final Object what, final boolean isRequest) {
        Sender ss;
        try {
            if (isRequest) {
                ss = this.senderMapForRequests.get(where);
                if (ss == null) {
                    ss = this.ms.getConnectionToReceiver(where, this.name.concat("-requestHandler"), SocketType.PUSH, false, true);
                    this.senderMapForRequests.put(where, ss);
                }
            }
            else {
                ss = this.senderMapForResponses.get(where);
                if (ss == null) {
                    ss = this.ms.getConnectionToReceiver(where, this.name.concat("-responseHandler"), SocketType.PUSH, false, true);
                    this.senderMapForResponses.put(where, ss);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Could not get connection to " + this.name + " on " + where, e);
        }
        try {
            ss.send(what);
        }
        catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            Executor.logger.warn((Object)(Thread.currentThread().getName() + " interrupted."));
        }
    }
    
    private <V> void sendResponse(final ExecutionRequest<O, ?> request, final ExecutionResponse<V> response) {
        this.send(request.from.getFrom(), response, false);
        this.updateStat(this.sentResponses, request.executable.getClass());
    }
    
    private void handleRequest(final ExecutionRequest<O, ?> request) {
        final Executable<O, ?> executable = request.executable;
        executable.setOn(this.on());
        this.updateStat(this.rcvdRequests, request.executable.getClass());
        ExecutionResponse<?> response = null;
        try {
            final Object result = executable.call();
            if (executable.hasResponse()) {
                response = new ExecutionResponse<Object>(request.requestId, result);
            }
        }
        catch (Throwable t) {
            response = new ExecutionResponse<Object>(request.requestId, t);
        }
        if (response != null) {
            this.sendResponse(request, response);
        }
    }
    
    private void handleResponse(final ExecutionResponse<?> response) {
        final ExecutableFuture<O, ?> future = this.futures.get(response.requestId);
        if (future != null) {
            future.setResponse(response);
            this.futures.remove(response.requestId);
            this.updateStat(this.rcvdResponses, future.request.executable.getClass());
        }
        else {
            Executor.logger.warn((Object)("Received response with no matching request id=" + response.requestId));
        }
    }
    
    @Override
    public void onMessage(final Object data) {
        if (data instanceof ExecutionRequest) {
            this.handleRequest((ExecutionRequest<O, ?>)data);
        }
        else if (data instanceof ExecutionResponse) {
            this.handleResponse((ExecutionResponse<?>)data);
        }
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public FlowComponent getOwner() {
        return null;
    }
    
    static {
        Executor.logger = Logger.getLogger((Class)Executor.class);
        Executor.requestIdSeq = new AtomicLong(1L);
    }
    
    class ExecutorMembershipListener implements MembershipListener
    {
        public void memberAdded(final MembershipEvent membershipEvent) {
        }
        
        public void memberRemoved(final MembershipEvent membershipEvent) {
            final UUID removedNode = new UUID(membershipEvent.getMember().getUuid());
            Executor.logger.info((Object)(Executor.this.name + ": Server " + removedNode + " has gone down"));
            final InterruptedException ie = new InterruptedException("Remote Server " + removedNode + " has terminated");
            for (final Map.Entry<Long, ExecutableFuture<O, ?>> futureEntry : Executor.this.futures.entrySet()) {
                if (futureEntry.getValue().where.equals((Object)removedNode)) {
                    Executor.logger.info((Object)(Executor.this.name + ": Cancelling future " + futureEntry.getValue().request.executable.getClass().getSimpleName()));
                    futureEntry.getValue().setResponse(new ExecutionResponse<Object>(futureEntry.getKey(), ie));
                }
            }
        }
        
        public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
        }
    }
    
    public static class ExecutionRequest<O, V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -3810004296646978979L;
        public From from;
        public long requestId;
        public Executable<O, V> executable;
        
        public ExecutionRequest() {
        }
        
        public ExecutionRequest(final From from, final long requestId, final Executable<O, V> executable) {
            this.from = from;
            this.requestId = requestId;
            this.executable = executable;
        }
        
        public void write(final Kryo kryo, final Output output) {
            this.from.write(kryo, output);
            output.writeLong(this.requestId);
            kryo.writeClassAndObject(output, (Object)this.executable);
        }
        
        public void read(final Kryo kryo, final Input input) {
            (this.from = new From()).read(kryo, input);
            this.requestId = input.readLong();
            this.executable = (Executable<O, V>)kryo.readClassAndObject(input);
        }
    }
    
    public static class ExecutionResponse<V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -5966990272616101600L;
        public long requestId;
        public V response;
        public Throwable exception;
        
        public ExecutionResponse() {
        }
        
        public ExecutionResponse(final long requestId, final V response) {
            this.requestId = requestId;
            this.response = response;
        }
        
        public ExecutionResponse(final long requestId, final Throwable exception) {
            this.requestId = requestId;
            this.exception = exception;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeLong(this.requestId);
            output.writeBoolean(this.response != null);
            if (this.response != null) {
                kryo.writeClassAndObject(output, (Object)this.response);
            }
            output.writeBoolean(this.exception != null);
            if (this.exception != null) {
                kryo.writeClassAndObject(output, (Object)this.exception);
            }
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.requestId = input.readLong();
            Throwable caughtException = null;
            try {
                final boolean hasResponse = input.readBoolean();
                if (hasResponse) {
                    this.response = (V)kryo.readClassAndObject(input);
                }
            }
            catch (Throwable t) {
                caughtException = new RuntimeException("Could not deserialize response object", t);
            }
            final boolean hasException = input.readBoolean();
            if (hasException) {
                this.exception = (Throwable)kryo.readClassAndObject(input);
            }
            if (caughtException != null) {
                this.exception = caughtException;
            }
        }
    }
    
    public static class ExecutableFuture<O, V> implements Future<V>, Latent
    {
        UUID where;
        ExecutionRequest<O, V> request;
        ExecutionResponse<V> response;
        private ReentrantLock responseLock;
        private Condition responseAvailable;
        long startTime;
        long endTime;
        boolean local;
        
        public ExecutableFuture(final ExecutionRequest<O, V> request, final UUID where) {
            this.responseLock = new ReentrantLock();
            this.responseAvailable = this.responseLock.newCondition();
            this.local = true;
            this.where = where;
            this.request = request;
            this.startTime = System.nanoTime();
        }
        
        public ExecutionRequest<O, V> getRequest() {
            return this.request;
        }
        
        public void setLocal(final boolean local) {
            this.local = local;
        }
        
        public boolean isLocal() {
            return this.local;
        }
        
        public UUID getWhere() {
            return this.where;
        }
        
        public void setResponse(final ExecutionResponse<?> response) {
            try {
                this.responseLock.lock();
                this.response = (ExecutionResponse<V>)response;
                this.endTime = System.nanoTime();
                this.responseAvailable.signalAll();
            }
            finally {
                this.responseLock.unlock();
            }
        }
        
        @Override
        public long getLatency() {
            return this.endTime - this.startTime;
        }
        
        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return false;
        }
        
        @Override
        public boolean isCancelled() {
            return false;
        }
        
        @Override
        public boolean isDone() {
            return !this.request.executable.hasResponse() || this.response != null;
        }
        
        @Override
        public V get() throws InterruptedException, ExecutionException {
            if (!this.request.executable.hasResponse()) {
                return null;
            }
            try {
                this.responseLock.lock();
                if (this.response == null) {
                    this.responseAvailable.await();
                }
            }
            finally {
                this.responseLock.unlock();
            }
            if (this.response.exception == null) {
                return this.response.response;
            }
            if (this.response.exception instanceof InterruptedException) {
                throw (InterruptedException)this.response.exception;
            }
            throw new ExecutionException("Execution on " + this.request.from + " failed.", this.response.exception);
        }
        
        @Override
        public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (!this.request.executable.hasResponse()) {
                return null;
            }
            try {
                this.responseLock.lock();
                if (this.response == null) {
                    this.responseAvailable.await(timeout, unit);
                }
            }
            finally {
                this.responseLock.unlock();
            }
            if (this.response.exception != null) {
                if (this.response.exception instanceof InterruptedException) {
                    throw (InterruptedException)this.response.exception;
                }
                throw new ExecutionException("Execution on " + this.request.from + " failed.", this.response.exception);
            }
            else {
                if (this.response == null) {
                    throw new TimeoutException("Execution on " + this.request.from + " timed out after " + timeout + " " + unit.name().toLowerCase());
                }
                return this.response.response;
            }
        }
    }
    
    public interface Latent
    {
        long getLatency();
    }
}
