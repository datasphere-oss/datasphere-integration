package com.datasphere.appmanager;

import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.ApiCallEvent;
import com.datasphere.appmanager.event.CommandConfirmationEvent;
import com.datasphere.appmanager.event.Event;
import com.datasphere.appmanager.event.MembershipEvent;
import com.datasphere.appmanager.event.NodeEvent;
import com.datasphere.appmanager.event.NodeEventCallback;
import com.hazelcast.core.IList;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.uuid.UUID;

public class EventQueueManager extends Thread
{
    public static final String NODE_EVENT_QUEUE = "#NodeEventQueue";
    private IList<byte[]> nodeEventQueue;
    private static Logger logger;
    private static EventQueueManager instance;
    private volatile NodeEventCallback callback;
    private volatile boolean running;
    
    public static EventQueueManager get() throws Exception {
        if (EventQueueManager.instance == null) {
            synchronized (EventQueueManager.class) {
                if (EventQueueManager.instance == null) {
                    EventQueueManager.instance = new EventQueueManager();
                }
            }
        }
        return EventQueueManager.instance;
    }
    
    private EventQueueManager() throws Exception {
        this.callback = null;
        this.nodeEventQueue = HazelcastSingleton.get().getList("#NodeEventQueue");
        if (this.nodeEventQueue == null) {
            throw new Exception("Could not get AppManagerQueue");
        }
    }
    
    private void sendEvent(final Event event) {
        final byte[] b = KryoSingleton.write(event, false);
        this.nodeEventQueue.add(b);
    }
    
    public void sendNodeEvent(final UUID serverId, final Event.EventAction EVENTACTION, final UUID flowId) {
        final NodeEvent nodeEvent = new NodeEvent(serverId, flowId, EVENTACTION);
        this.sendEvent(nodeEvent);
    }
    
    public void sendCommandConfirmationEvent(final UUID serverId, final Event.EventAction EVENTACTION, final UUID flowId, final Long commandTimestamp) {
        final CommandConfirmationEvent event = new CommandConfirmationEvent(serverId, flowId, EVENTACTION, commandTimestamp);
        this.sendEvent(event);
    }
    
    public void sendNodeErrorEvent(final UUID serverId, final Event.EventAction EVENTACTION, final ExceptionEvent e, final UUID flowId) {
        final NodeEvent nodeEvent = new NodeEvent(serverId, flowId, EVENTACTION, e);
        this.sendEvent(nodeEvent);
    }
    
    public void sendApiEvent(final UUID requestId, final ActionType what, final Map<String, Object> params, final UUID flowId) {
        final ApiCallEvent apiCallEvent = new ApiCallEvent(requestId, what, params, flowId);
        this.sendEvent(apiCallEvent);
    }
    
    public void addMembershipEventToQueue(final Event.EventAction event, final UUID memberUUID) {
        this.sendEvent(new MembershipEvent(event, memberUUID));
    }
    
    public void addItemListener(final NodeEventCallback appManagerWorker) {
        if (this.callback != null) {
            throw new IllegalStateException("Only one listener allowed on NodeEventQueue. Callback is already not null");
        }
        this.callback = appManagerWorker;
        this.running = true;
        this.start();
    }
    
    public void removeItemListener() {
        this.running = false;
        this.interrupt();
        try {
            this.join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.callback = null;
    }
    
    @Override
    public void run() {
        while (this.running) {
            try {
                while (this.nodeEventQueue.isEmpty()) {
                    Thread.sleep(100L);
                }
                final byte[] eventBytes = (byte[])this.nodeEventQueue.get(0);
                final Event event = (Event)KryoSingleton.read(eventBytes, false);
                this.callback.call(event);
                this.nodeEventQueue.remove(0);
                continue;
            }
            catch (InterruptedException e) {
                EventQueueManager.logger.error((Object)e);
            }
            break;
        }
    }
    
    static {
        EventQueueManager.logger = Logger.getLogger((Class)EventQueueManager.class);
    }
}
