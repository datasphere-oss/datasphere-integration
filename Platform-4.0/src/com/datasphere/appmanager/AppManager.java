package com.datasphere.appmanager;

import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.uuid.UUID;

public final class AppManager
{
    private final BaseServer srv;
    private static Logger logger;
    private AppManagerWorker appManagerWorker;
    
    public AppManager(final BaseServer srv) throws Exception {
        AppManager.logger.info((Object)"AppManager starting");
        this.srv = srv;
        (this.appManagerWorker = new AppManagerWorker(srv)).start();
    }
    
    public synchronized void stop() {
        AppManager.logger.info((Object)"AppManager Stopping ");
        this.appManagerWorker.stopAppManager();
    }
    
    public void changeApplicationState(final ActionType what, final UUID flowId, final Map<String, Object> params, final UUID requestId) throws Exception {
        AppManager.logger.info((Object)("Change DSS Application State: " + what + ": " + flowId));
        EventQueueManager.get().sendApiEvent(requestId, what, params, flowId);
    }
    
    public void scheduleCheckpoint(final UUID flowId) {
        this.appManagerWorker.scheduleCheckpoint(flowId);
    }
    
    public void scheduleCheckpoint(final UUID flowId, final long delayAdvance) {
        this.appManagerWorker.scheduleCheckpoint(flowId, delayAdvance);
    }
    
    public void unscheduleCheckpoint(final UUID flowId) {
        this.appManagerWorker.unscheduleCheckpoint(flowId);
    }
    
    static {
        AppManager.logger = Logger.getLogger((Class)AppManager.class);
    }
    
    public enum ComponentStatus
    {
        UNKNOWN, 
        INITIALIZED, 
        STARTED, 
        STOPPED, 
        CRASHED;
    }
    
    public enum EventAction
    {
        START, 
        STOP, 
        UNDEPLOY, 
        DEPLOY, 
        RESTART, 
        FIXERROR, 
        BOOT, 
        NODEUPDATE, 
        NODEDELETED;
    }
}
