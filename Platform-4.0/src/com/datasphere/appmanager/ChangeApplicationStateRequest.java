package com.datasphere.appmanager;

import java.util.Map;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.Server;
import com.datasphere.uuid.UUID;

public class ChangeApplicationStateRequest implements RemoteCall<Context.Result>
{
    private static final long serialVersionUID = -8137681093645820789L;
    private static Logger logger;
    private final ActionType what;
    private final UUID flowId;
    private final Map<String, Object> params;
    private final UUID requestId;
    private final UUID appManagerId;
    
    public ChangeApplicationStateRequest(final ActionType what, final UUID flowId, final Map<String, Object> params, final UUID requestId, final UUID appManagerId) {
        this.what = what;
        this.flowId = flowId;
        this.params = params;
        this.requestId = requestId;
        this.appManagerId = appManagerId;
    }
    
    public UUID getAppManagerId() {
        return this.appManagerId;
    }
    
    @Override
    public Context.Result call() throws Exception {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Member m = hz.getCluster().getLocalMember();
        if (HazelcastSingleton.isClientMember()) {
            return null;
        }
        final Server srv = Server.server;
        if (srv != null) {
            final Context.Result r = new Context.Result();
            if (ChangeApplicationStateRequest.logger.isInfoEnabled()) {
                ChangeApplicationStateRequest.logger.info((Object)("Got request " + this.what.toString() + " for flow " + this.flowId));
            }
            srv.getAppManager().changeApplicationState(this.what, this.flowId, this.params, this.requestId);
            r.processedObjects = srv.getDeployedObjects(this.flowId);
            r.serverID = srv.getServerID();
            if (this.what == ActionType.STATUS) {
                r.runningStatus = srv.whatIsRunning(this.flowId);
            }
            return r;
        }
        throw new RuntimeException("non-lite member " + m + " has no running server");
    }
    
    static {
        ChangeApplicationStateRequest.logger = Logger.getLogger((Class)ChangeApplicationStateRequest.class);
    }
}
