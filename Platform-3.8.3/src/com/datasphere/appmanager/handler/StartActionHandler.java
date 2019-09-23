package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;

import java.util.*;
import com.datasphere.runtime.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;

public class StartActionHandler extends BaseActionHandler
{
    private static Logger logger;
    
    public StartActionHandler(final boolean isApi) {
        super(isApi);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        StartActionHandler.logger.info((Object)("Starting the application " + appContext.getFlow().getFullName() + " completed"));
        final MetaInfo.StatusInfo.Status oldDesiredStatus = appContext.getDesiredAppStatus();
        try {
            if (this.isApi()) {
                appContext.updateDesiredStatus(MetaInfo.StatusInfo.Status.RUNNING);
            }
            List<Property> params = null;
            if (event != null && event instanceof ApiCallEvent) {
                final ApiCallEvent apiCallEvent = (ApiCallEvent)event;
                final Object value = apiCallEvent.getParams().get("RECOVERYDESCRIPTION");
                if (value != null) {
                    params = new ArrayList<Property>();
                    params.add(new Property("RecoveryDescription", value));
                    if (params != null) {
                        params.add(new Property("StartupSignalsSetName", "StartupSignals" + apiCallEvent.getRequestId().toString()));
                    }
                }
            }
            appContext.execute(ActionType.START, params, true);
            try {
                appContext.waitForSubscriptionsCompleted();
                this.startSources(params, appContext);
            }
            catch (Exception e) {
                appContext.execute(ActionType.STOP, null, false);
                throw e;
            }
            Server.server.getAppManager().scheduleCheckpoint(appContext.getAppId());
            return MetaInfo.StatusInfo.Status.RUNNING;
        }
        catch (Exception e2) {
            if (this.isApi()) {
                appContext.updateDesiredStatus(oldDesiredStatus);
            }
            appContext.execute(ActionType.STOP_CACHES, null, false);
            throw e2;
        }
        finally {
            if (event != null && event instanceof ApiCallEvent) {
                final ApiCallEvent apiCallEvent2 = (ApiCallEvent)event;
                if (apiCallEvent2.getRequestId() != null) {
                    final ISet m = HazelcastSingleton.get().getSet("StartupSignals" + apiCallEvent2.getRequestId().toString());
                    m.destroy();
                }
            }
        }
    }
    
    private void startSources(final List<Property> params, final AppContext appContext) throws MetaDataRepositoryException {
        StartActionHandler.logger.info((Object)("Starting sources for application " + appContext.getFlow().getFullName()));
        appContext.execute(ActionType.START_SOURCES, params, false);
        StartActionHandler.logger.info((Object)("Starting sources completed. for application " + appContext.getFlow().getFullName() + " done."));
    }
    
    static {
        StartActionHandler.logger = Logger.getLogger((Class)StartActionHandler.class);
    }
}
