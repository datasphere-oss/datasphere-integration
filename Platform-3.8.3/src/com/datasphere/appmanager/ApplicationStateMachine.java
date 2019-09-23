package com.datasphere.appmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.Event;
import com.datasphere.appmanager.handler.ActionHandler;
import com.datasphere.appmanager.handler.AddToDeferredActionHandler;
import com.datasphere.appmanager.handler.DeployActionHandler;
import com.datasphere.appmanager.handler.DiscardCheckpointActionHandler;
import com.datasphere.appmanager.handler.ErrorDeployActionHandler;
import com.datasphere.appmanager.handler.ErrorRunningActionHandler;
import com.datasphere.appmanager.handler.IdempotentActionHandler;
import com.datasphere.appmanager.handler.IdempotentThrowExceptionActionHandler;
import com.datasphere.appmanager.handler.InitiateCheckpointActionHandler;
import com.datasphere.appmanager.handler.NESNodeDeletedActionHandler;
import com.datasphere.appmanager.handler.NESNodeUpdateActionHandler;
import com.datasphere.appmanager.handler.NoOpActionHandler;
import com.datasphere.appmanager.handler.NodeAddDeployedActionHandler;
import com.datasphere.appmanager.handler.NodeAddErrorActionHandler;
import com.datasphere.appmanager.handler.NodeAddRunningActionHandler;
import com.datasphere.appmanager.handler.NodeDeleteDeployedActionHandler;
import com.datasphere.appmanager.handler.NodeDeleteErrorActionHandler;
import com.datasphere.appmanager.handler.NodeDeleteRunningActionHandler;
import com.datasphere.appmanager.handler.NodeRemoveTransientStateActionHandler;
import com.datasphere.appmanager.handler.QuiesceActionHandler;
import com.datasphere.appmanager.handler.ResumeApiActionHandler;
import com.datasphere.appmanager.handler.SoftErrorDeployActionHandler;
import com.datasphere.appmanager.handler.SoftErrorRunningActionHandler;
import com.datasphere.appmanager.handler.StartActionHandler;
import com.datasphere.appmanager.handler.StopActionHandler;
import com.datasphere.appmanager.handler.UnDeployActionHandler;
import com.datasphere.appmanager.handler.WaitCheckpointingActionHandler;
import com.datasphere.appmanager.handler.WaitDeployingActionHandler;
import com.datasphere.appmanager.handler.WaitQuiescingCheckpointingActionHandler;
import com.datasphere.appmanager.handler.WaitQuiescingFlushingActionHandler;
import com.datasphere.exception.ActionNotFoundWarning;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class ApplicationStateMachine
{
    private static Logger logger;
    private static final Map<MetaInfo.StatusInfo.Status, Map<Event.EventAction, ActionHandler>> actionHandlerMap;
    private static final Map<MetaInfo.StatusInfo.Status, Map<MetaInfo.StatusInfo.Status, Event.EventAction>> desiredStateActionMap;
    
    public static void runStateFlow(final Event.EventAction actionType, final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        takeAction(actionType, appContext, event, managedNodes);
        while (true) {
            final MetaInfo.StatusInfo.Status desiredStatus = appContext.getDesiredAppStatus();
            final MetaInfo.StatusInfo.Status currentStatus = appContext.getCurrentStatus();
            if (currentStatus.isSync()) {
                final Event deferredEvent = appContext.getNextEventFromDeferredQueue();
                if (deferredEvent != null) {
                    takeAction(deferredEvent.getEventAction(), appContext, deferredEvent, managedNodes);
                    continue;
                }
            }
            final Map<MetaInfo.StatusInfo.Status, Event.EventAction> actionMap = ApplicationStateMachine.desiredStateActionMap.get(currentStatus);
            if (actionMap == null) {
                break;
            }
            final Event.EventAction action = actionMap.get(desiredStatus);
            if (action == null) {
                break;
            }
            takeAction(action, appContext, event, managedNodes);
        }
    }
    
    private static void takeAction(final Event.EventAction actionType, final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        ApplicationStateMachine.logger.info((Object)("Taking Action " + actionType + " for application " + appContext + " Event: " + event));
        final MetaInfo.StatusInfo.Status currentStatus = appContext.getCurrentStatus();
        final ActionHandler actionHandler = ApplicationStateMachine.actionHandlerMap.get(currentStatus).get(actionType);
        if (actionHandler != null) {
            ApplicationStateMachine.logger.info((Object)("Calling ActionHandler " + actionHandler));
            final MetaInfo.StatusInfo.Status newState = actionHandler.handle(appContext, event, managedNodes);
            appContext.updateStatus(newState);
            ApplicationStateMachine.logger.info((Object)("Action " + actionType + " for application " + appContext + " returned " + newState));
            return;
        }
        String action = actionType.toString();
        action = action.replaceFirst("API_", "");
        throw new ActionNotFoundWarning("Failed to " + action + " application " + appContext.getAppName() + " because application is " + currentStatus);
    }
    
    static {
        ApplicationStateMachine.logger = Logger.getLogger((Class)ApplicationStateMachine.class);
        actionHandlerMap = new HashMap<MetaInfo.StatusInfo.Status, Map<Event.EventAction, ActionHandler>>();
        desiredStateActionMap = new HashMap<MetaInfo.StatusInfo.Status, Map<MetaInfo.StatusInfo.Status, Event.EventAction>>();
        Map<Event.EventAction, ActionHandler> stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.API_DEPLOY, new DeployActionHandler(true));
        stateHandlerMap.put(Event.EventAction.SOFT_DEPLOY, new DeployActionHandler(false));
        stateHandlerMap.put(Event.EventAction.API_CHECKPOINT, new NoOpActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.CREATED, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.NODE_ERROR, new NodeRemoveTransientStateActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new AddToDeferredActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NodeRemoveTransientStateActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_CACHE_DEPLOYED, new WaitDeployingActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.DEPLOYING, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.NODE_ERROR, new NodeRemoveTransientStateActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new AddToDeferredActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NodeRemoveTransientStateActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_QUIESCE_FLUSHED, new WaitQuiescingFlushingActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED, new WaitQuiescingCheckpointingActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_CHECKPOINTED, new WaitCheckpointingActionHandler());
        stateHandlerMap.put(Event.EventAction.API_CHECKPOINT, new NoOpActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.QUIESCING, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.API_UNDEPLOY, new UnDeployActionHandler(false, true));
        stateHandlerMap.put(Event.EventAction.SOFT_UNDEPLOY, new UnDeployActionHandler(false, false));
        stateHandlerMap.put(Event.EventAction.API_DEPLOY, new IdempotentThrowExceptionActionHandler());
        stateHandlerMap.put(Event.EventAction.SOFT_DEPLOY, new IdempotentActionHandler());
        stateHandlerMap.put(Event.EventAction.API_START, new StartActionHandler(true));
        stateHandlerMap.put(Event.EventAction.SOFT_START, new StartActionHandler(false));
        stateHandlerMap.put(Event.EventAction.API_STOP, new IdempotentThrowExceptionActionHandler());
        stateHandlerMap.put(Event.EventAction.API_QUIESCE, new IdempotentActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_CACHE_DEPLOYED, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ERROR, new ErrorDeployActionHandler());
        stateHandlerMap.put(Event.EventAction.SOFT_ERROR, new SoftErrorDeployActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NodeDeleteDeployedActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new NodeAddDeployedActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_CHECKPOINTED, new DiscardCheckpointActionHandler());
        stateHandlerMap.put(Event.EventAction.API_CHECKPOINT, new NoOpActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.DEPLOYED, stateHandlerMap);
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.QUIESCED, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.API_STOP, new StopActionHandler(true));
        stateHandlerMap.put(Event.EventAction.SOFT_STOP, new StopActionHandler(false));
        stateHandlerMap.put(Event.EventAction.API_START, new IdempotentThrowExceptionActionHandler());
        stateHandlerMap.put(Event.EventAction.API_RESUME, new IdempotentThrowExceptionActionHandler());
        stateHandlerMap.put(Event.EventAction.API_QUIESCE, new QuiesceActionHandler(true));
        stateHandlerMap.put(Event.EventAction.SOFT_START, new IdempotentActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_CACHE_DEPLOYED, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_RUNNING, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ERROR, new ErrorRunningActionHandler());
        stateHandlerMap.put(Event.EventAction.SOFT_ERROR, new SoftErrorRunningActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NodeDeleteRunningActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new NodeAddRunningActionHandler());
        stateHandlerMap.put(Event.EventAction.API_CHECKPOINT, new InitiateCheckpointActionHandler(true));
        stateHandlerMap.put(Event.EventAction.NODE_APP_CHECKPOINTED, new WaitCheckpointingActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.RUNNING, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.API_UNDEPLOY, new UnDeployActionHandler(true, true));
        stateHandlerMap.put(Event.EventAction.SOFT_UNDEPLOY, new UnDeployActionHandler(true, false));
        stateHandlerMap.put(Event.EventAction.API_RESUME, new ResumeApiActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_CACHE_DEPLOYED, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ERROR, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.SOFT_ERROR, new NoOpActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NodeDeleteErrorActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new NodeAddErrorActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_CHECKPOINTED, new DiscardCheckpointActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.CRASH, stateHandlerMap);
        stateHandlerMap = new HashMap<Event.EventAction, ActionHandler>();
        stateHandlerMap.put(Event.EventAction.NODE_DELETED, new NESNodeDeletedActionHandler());
        stateHandlerMap.put(Event.EventAction.API_UNDEPLOY, new UnDeployActionHandler(true, true));
        stateHandlerMap.put(Event.EventAction.SOFT_UNDEPLOY, new UnDeployActionHandler(true, false));
        stateHandlerMap.put(Event.EventAction.NODE_ADDED, new NESNodeUpdateActionHandler());
        stateHandlerMap.put(Event.EventAction.SOFT_ERROR, new SoftErrorDeployActionHandler());
        stateHandlerMap.put(Event.EventAction.NODE_APP_CHECKPOINTED, new DiscardCheckpointActionHandler());
        ApplicationStateMachine.actionHandlerMap.put(MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS, stateHandlerMap);
        Map<MetaInfo.StatusInfo.Status, Event.EventAction> actionMap = new HashMap<MetaInfo.StatusInfo.Status, Event.EventAction>();
        actionMap.put(MetaInfo.StatusInfo.Status.DEPLOYED, Event.EventAction.SOFT_DEPLOY);
        actionMap.put(MetaInfo.StatusInfo.Status.QUIESCED, Event.EventAction.SOFT_DEPLOY);
        ApplicationStateMachine.desiredStateActionMap.put(MetaInfo.StatusInfo.Status.CREATED, actionMap);
        actionMap = new HashMap<MetaInfo.StatusInfo.Status, Event.EventAction>();
        actionMap.put(MetaInfo.StatusInfo.Status.RUNNING, Event.EventAction.SOFT_START);
        actionMap.put(MetaInfo.StatusInfo.Status.CREATED, Event.EventAction.SOFT_UNDEPLOY);
        ApplicationStateMachine.desiredStateActionMap.put(MetaInfo.StatusInfo.Status.DEPLOYED, actionMap);
        actionMap = new HashMap<MetaInfo.StatusInfo.Status, Event.EventAction>();
        actionMap.put(MetaInfo.StatusInfo.Status.DEPLOYED, Event.EventAction.SOFT_STOP);
        ApplicationStateMachine.desiredStateActionMap.put(MetaInfo.StatusInfo.Status.RUNNING, actionMap);
    }
}
