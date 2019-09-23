package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class QuiesceActionHandler extends BaseActionHandler
{
    private static Logger logger;
    
    public QuiesceActionHandler(final boolean isApi) {
        super(isApi);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> nodes) throws Exception {
        Server.server.getAppManager().unscheduleCheckpoint(appContext.getAppId());
        final MetaInfo.StatusInfo.Status originalDesiredStatus = appContext.getDesiredAppStatus();
        try {
            appContext.execute(ActionType.PAUSE_SOURCES, false, nodes);
            appContext.execute(ActionType.APPROVE_QUIESCE, false, nodes);
        }
        catch (Exception e) {
            QuiesceActionHandler.logger.error((Object)e.getMessage());
            appContext.updateDesiredStatus(originalDesiredStatus);
            appContext.execute(ActionType.UNPAUSE_SOURCES, false);
            throw new Exception("Could not quiesce application because a source did not approve: " + e.getMessage());
        }
        final long commandTimestamp = System.currentTimeMillis();
        final List<Property> params = new ArrayList<Property>();
        params.add(new Property("COMMAND_TIMESTAMP", commandTimestamp));
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("Starting a Quiesce Flush for app " + appContext.getAppName() + " at timestamp " + commandTimestamp));
        }
        appContext.execute(ActionType.QUIESCE_FLUSH, nodes, params, false);
        appContext.startWaitingForResponses(new HashSet<UUID>(nodes));
        return MetaInfo.StatusInfo.Status.QUIESCING;
    }
    
    static {
        QuiesceActionHandler.logger = Logger.getLogger("Commands");
    }
}
