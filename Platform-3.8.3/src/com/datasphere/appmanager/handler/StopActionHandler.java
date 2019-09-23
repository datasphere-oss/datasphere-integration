package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;

public class StopActionHandler extends BaseActionHandler
{
    public StopActionHandler(final boolean isApi) {
        super(isApi);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        Server.server.getAppManager().unscheduleCheckpoint(appContext.getAppId());
        if (this.isApi()) {
            appContext.updateDesiredStatus(MetaInfo.StatusInfo.Status.DEPLOYED);
        }
        appContext.execute(ActionType.STOP, false);
        appContext.execute(ActionType.START_CACHES, true);
        return MetaInfo.StatusInfo.Status.DEPLOYED;
    }
}
