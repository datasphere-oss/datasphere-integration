package com.datasphere.appmanager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.datasphere.exception.Warning;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class AppManagerRequestClient implements EntryListener<Long, UUID>
{
    private static final Logger logger;
    public static final String APP_MANAGER_RESPONSE = "#AppManagerResponse";
    private final AuthToken sessionId;
    private final String listenerId;
    private ReentrantLock responseLock;
    private Condition responseAvailable;
    Map<UUID, ChangeApplicationStateRequest> requestMap;
    Map<UUID, ChangeApplicationStateResponse> responseMap;
    private ITopic<ChangeApplicationStateResponse> appManagerResponseTopic;
    private String topicListenerId;
    
    private AppManagerRequestClient() {
        this.responseLock = new ReentrantLock();
        this.responseAvailable = this.responseLock.newCondition();
        this.requestMap = new HashMap<UUID, ChangeApplicationStateRequest>();
        this.responseMap = new HashMap<UUID, ChangeApplicationStateResponse>();
        this.sessionId = null;
        this.listenerId = null;
    }
    
    public AppManagerRequestClient(final AuthToken sessionID) {
        this.responseLock = new ReentrantLock();
        this.responseAvailable = this.responseLock.newCondition();
        this.requestMap = new HashMap<UUID, ChangeApplicationStateRequest>();
        this.responseMap = new HashMap<UUID, ChangeApplicationStateResponse>();
        this.sessionId = sessionID;
        this.listenerId = AppManagerServerLocation.registerListerForAppManagerUpdate((EntryListener<Long, UUID>)this);
        this.setAppManagerResponseCallback((MessageListener<ChangeApplicationStateResponse>)new AppManagerResponseListener());
    }
    
    public void close() {
        AppManagerServerLocation.unRegisterListerForAppManagerUpdate(this.listenerId);
        if (this.appManagerResponseTopic != null && this.topicListenerId != null) {
            this.appManagerResponseTopic.removeMessageListener(this.topicListenerId);
        }
    }
    
    private synchronized void setResponse(final ChangeApplicationStateResponse response) {
        try {
            this.responseLock.lock();
            final UUID requestId = response.getRequestId();
            if (this.responseMap.get(requestId) != null) {
                AppManagerRequestClient.logger.warn((Object)("Response already set when trying to setResponse for request " + this.requestMap.get(requestId)));
            }
            this.responseMap.put(requestId, response);
            this.responseAvailable.signalAll();
        }
        finally {
            this.responseLock.unlock();
        }
    }
    
    private ChangeApplicationStateResponse get(final UUID queryUUID) {
        ChangeApplicationStateResponse response = null;
        try {
            this.responseLock.lock();
            response = this.responseMap.get(queryUUID);
            while (response == null) {
                try {
                    this.responseAvailable.await();
                    response = this.responseMap.get(queryUUID);
                    continue;
                }
                catch (InterruptedException e) {
                    return new ChangeApplicationStateResponse(queryUUID, ChangeApplicationStateResponse.RESULT.FAILURE, e.getMessage());
                }
            }
        }
        finally {
            this.responseLock.unlock();
            this.responseMap.remove(queryUUID);
            this.requestMap.remove(queryUUID);
        }
        return response;
    }
    
    private void setAppManagerResponseCallback(final MessageListener<ChangeApplicationStateResponse> listener) {
        if (this.appManagerResponseTopic == null) {
            this.appManagerResponseTopic = HazelcastSingleton.get().getTopic("#AppManagerResponse");
        }
        if (listener != null) {
            this.topicListenerId = this.appManagerResponseTopic.addMessageListener((MessageListener)listener);
        }
    }
    
    private Collection<Context.Result> execute(final Callable<Context.Result> action, final UUID serverId) {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        final Map<UUID, Member> valid = new HashMap<UUID, Member>();
        for (final Member member : srvs) {
            valid.put(new UUID(member.getUuid()), member);
        }
        final Set<Member> executeSrvs = new HashSet<Member>();
        Member member = valid.get(serverId);
        if (member != null) {
            executeSrvs.add(member);
            return DistributedExecutionManager.exec(hz, action, executeSrvs);
        }
        final StringBuilder keysHC = new StringBuilder();
        for (final UUID key : valid.keySet()) {
            keysHC.append(key.hashCode() + " ");
        }
        AppManagerRequestClient.logger.error((Object)("Server " + serverId + " with hashcode " + serverId.hashCode() + " not found. Current Valid members are " + valid.toString()));
        AppManagerRequestClient.logger.error((Object)("Hash code for keys in the array " + keysHC.toString()));
        throw new Warning("Server " + serverId + "=" + serverId.hashCode() + " not found. Current Valid members are " + valid.toString());
    }
    
    public ChangeApplicationStateResponse sendRequest(final ActionType what, final MetaInfo.Flow flow, final Map<String, Object> params) throws Exception {
        return this.sendRequest(what, flow.getUuid(), params);
    }
    
    public ChangeApplicationStateResponse sendRequest(final ActionType what, final UUID flowId, final Map<String, Object> params) throws Exception {
        final UUID queryUUID = new UUID(System.currentTimeMillis());
        UUID appManagerId = null;
        int attemptCount = 0;
        while (attemptCount < 20) {
            appManagerId = AppManagerServerLocation.getAppManagerServerId();
            if (appManagerId == null) {
                try {
                    Thread.sleep(200L);
                    ++attemptCount;
                    continue;
                }
                catch (InterruptedException e) {}
                break;
            }
            break;
        }
        if (appManagerId == null) {
            throw new Warning("No AppManager found in the cluster");
        }
        final ChangeApplicationStateRequest changeStateRequest = new ChangeApplicationStateRequest(what, flowId, params, queryUUID, appManagerId);
        this.requestMap.put(queryUUID, changeStateRequest);
        final Collection<Context.Result> results = this.execute(changeStateRequest, appManagerId);
        final ChangeApplicationStateResponse result = this.get(queryUUID);
        if (result.getResult() != ChangeApplicationStateResponse.RESULT.SUCCESS) {
            if (AppManagerRequestClient.logger.isDebugEnabled()) {
                AppManagerRequestClient.logger.debug((Object)("Failed to get response from appManager " + appManagerId));
            }
            throw new Warning(result.getExceptionMsg());
        }
        return result;
    }
    
    private void handle(final EntryEvent<Long, UUID> event) {
        for (final Map.Entry<UUID, ChangeApplicationStateRequest> entry : this.requestMap.entrySet()) {
            if (event.getValue() == entry.getValue().getAppManagerId()) {
                this.setResponse(new ChangeApplicationStateResponse(entry.getKey(), ChangeApplicationStateResponse.RESULT.FAILURE, "AppManager failure..Please try again"));
            }
        }
    }
    
    public void entryAdded(final EntryEvent<Long, UUID> event) {
    }
    
    public void entryRemoved(final EntryEvent<Long, UUID> event) {
        this.handle(event);
    }
    
    public void entryUpdated(final EntryEvent<Long, UUID> event) {
        this.handle(event);
    }
    
    public void entryEvicted(final EntryEvent<Long, UUID> event) {
        this.handle(event);
    }
    
    public void mapEvicted(final MapEvent event) {
    }
    
    public void mapCleared(final MapEvent event) {
    }
    
    static {
        logger = Logger.getLogger(AppManagerRequestClient.class.getName());
    }
    
    class AppManagerResponseListener implements MessageListener<ChangeApplicationStateResponse>
    {
        public void onMessage(final Message<ChangeApplicationStateResponse> message) {
            if (AppManagerRequestClient.logger.isDebugEnabled()) {
                AppManagerRequestClient.logger.debug((Object)("got response message " + message.getMessageObject()));
            }
            final ChangeApplicationStateResponse messageObject = (ChangeApplicationStateResponse)message.getMessageObject();
            final UUID requestId = messageObject.getRequestId();
            synchronized (AppManagerRequestClient.this.responseMap) {
                AppManagerRequestClient.this.setResponse(messageObject);
            }
        }
    }
}
