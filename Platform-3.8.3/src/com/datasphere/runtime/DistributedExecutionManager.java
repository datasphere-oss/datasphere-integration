package com.datasphere.runtime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.datasphere.exception.Warning;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.exceptions.ConnectionException;

public class DistributedExecutionManager
{
    private static Logger logger;
    private static long timeout;
    
    public static Set<Member> getServerByAddress(final HazelcastInstance hz, final String server) {
        for (final Member m : hz.getCluster().getMembers()) {
            final String ip = m.getInetSocketAddress().toString();
            if (server.equals(ip)) {
                return Collections.singleton(m);
            }
        }
        return Collections.emptySet();
    }
    
    public static Set<Member> getAllServers(final HazelcastInstance hz) {
        return (Set<Member>)hz.getCluster().getMembers();
    }
    
    public static Set<Member> getAllServersButMe(final HazelcastInstance hz) {
        final Set<Member> mems = new HashSet<Member>(hz.getCluster().getMembers());
        mems.remove(hz.getCluster().getLocalMember());
        return mems;
    }
    
    public static Set<Member> getFirstServer(final HazelcastInstance hz) {
        final Iterator<Member> iterator = hz.getCluster().getMembers().iterator();
        if (iterator.hasNext()) {
            final Member m = iterator.next();
            return Collections.singleton(m);
        }
        return Collections.emptySet();
    }
    
    public static Set<Member> getServerByPartKey(final HazelcastInstance hz, final Object key) {
        final Member m = hz.getPartitionService().getPartition(key).getOwner();
        if (m != null) {
            return Collections.singleton(m);
        }
        return getFirstServer(hz);
    }
    
    public static Set<Member> getLocalServer(final HazelcastInstance hz) {
        return Collections.singleton(hz.getCluster().getLocalMember());
    }
    
    public static <T extends Serializable> void sendMessage(final HazelcastInstance hz, final Set<UUID> agentNodeIds, final T message) {
        for (final UUID nodeId : agentNodeIds) {
            final ITopic<T> topic = (ITopic<T>)hz.getTopic("node-" + nodeId.toString());
            topic.publish(message);
        }
    }
    
    public static <T> Collection<T> exec(final HazelcastInstance hz, final Callable<T> task, final Set<Member> srvs) {
        if (DistributedExecutionManager.timeout == -1L) {
            DistributedExecutionManager.timeout = Integer.parseInt(System.getProperty("com.datasphere.config.appmanager.synchronous.timeout", "300"));
        }
        final IExecutorService executorService = hz.getExecutorService("executor");
        long waitTime = DistributedExecutionManager.timeout;
        try {
            final Map<Member, Future<T>> futureMap = (Map<Member, Future<T>>)executorService.submitToMembers((Callable)task, (Collection)srvs);
            TimeoutException timeoutException = null;
            final List<T> list = new ArrayList<T>(futureMap.size());
            for (final Member m : futureMap.keySet()) {
                final Future<T> future = futureMap.get(m);
                T result;
                try {
                    result = future.get(waitTime, TimeUnit.SECONDS);
                }
                catch (TimeoutException e2) {
                    DistributedExecutionManager.logger.error((Object)("Timeout while waiting for a remote call on member " + m + " for task " + task));
                    future.cancel(true);
                    timeoutException = new TimeoutException("Timeout while waiting for a remote call on member " + m + " for task " + task);
                    waitTime = 1L;
                    continue;
                }
                list.add(result);
            }
            if (timeoutException != null) {
                throw timeoutException;
            }
            return list;
        }
        catch (ExecutionException | InterruptedException | TimeoutException ex2) {
            if (ex2.getCause() instanceof Warning) {
                throw (Warning)ex2.getCause();
            }
            throw new RuntimeException(ex2);
        }
    }
    
    public static <T> T execOnAny(final HazelcastInstance hz, final Callable<T> task) {
        final IExecutorService executorService = hz.getExecutorService("executor");
        int count = 0;
        while (true) {
            try {
                final Future<T> future = (Future<T>)executorService.submit((Callable)task);
                return future.get();
            }
            catch (ExecutionException | InterruptedException ex2) {
                if (ex2.getCause() instanceof TargetDisconnectedException) {
                    if (count >= 5) {
                        throw new RuntimeException(ex2);
                    }
                    ++count;
                    continue;
                }
                else {
                    if (ex2.getCause() instanceof Warning) {
                        throw (Warning)ex2.getCause();
                    }
                    throw new RuntimeException(ex2);
                }
            }
            catch (ConnectionException e2) {
                if (count >= 5) {
                    throw new RuntimeException(e2);
                }
                ++count;
                continue;
            }
        }
    }
    
    static {
        DistributedExecutionManager.logger = Logger.getLogger((Class)DistributedExecutionManager.class);
        DistributedExecutionManager.timeout = -1L;
    }
}
