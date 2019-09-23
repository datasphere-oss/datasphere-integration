package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.exception.*;
import com.datasphere.metaRepository.*;

public class IdempotentThrowExceptionActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws MetaDataRepositoryException {
        if (event.getEventAction() == Event.EventAction.API_START || event.getEventAction() == Event.EventAction.API_RESUME) {
            throw new Warning("Application " + appContext.getAppName() + " is already running");
        }
        if (event.getEventAction() == Event.EventAction.API_DEPLOY || event.getEventAction() == Event.EventAction.API_STOP) {
            throw new Warning("Application " + appContext.getAppName() + " is already deployed");
        }
        throw new Warning("Nothing to do");
    }
}
