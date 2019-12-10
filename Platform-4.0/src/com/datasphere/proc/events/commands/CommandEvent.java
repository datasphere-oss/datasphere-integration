package com.datasphere.proc.events.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.anno.EventType;
import com.datasphere.proc.records.Record;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.containers.DefaultBatch;
import com.datasphere.runtime.containers.DefaultTaskEvent;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:DSSCommandEvent:1.0")
public abstract class CommandEvent extends DefaultTaskEvent
{
    private static Logger logger;
    protected static final Map<String, List<CommandEvent>> eventsInWaiting;
    private static final Object commandDataObject;
    private static final Record commandDataEvent;
    public UUID appUuid;
    protected long commandTimestamp;
    
    private CommandEvent() {
        super(CommandEvent.commandDataEvent);
        this.commandTimestamp = -1L;
    }
    
    public CommandEvent(final UUID appUuid, final long commandTimestamp) {
        super(CommandEvent.commandDataEvent);
        this.commandTimestamp = -1L;
        this.appUuid = appUuid;
        this.commandTimestamp = commandTimestamp;
    }
    
    public ITaskEvent.EVENTTYPE getEventType() {
        return ITaskEvent.EVENTTYPE.CommandEvent;
    }
    
    public void write(final Kryo kryo, final Output output) {
        kryo.writeClassAndObject(output, (Object)this.appUuid);
        kryo.writeClassAndObject(output, (Object)this.commandTimestamp);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.appUuid = (UUID)kryo.readClassAndObject(input);
        this.commandTimestamp = (long)kryo.readClassAndObject(input);
    }
    
    public abstract boolean performCommand(final FlowComponent p0) throws Exception;
    
    public abstract boolean performCommandForStream(final FlowComponent p0, final long p1) throws Exception;
    
    protected static synchronized boolean shouldWait(final String metaId, final int indegree, final FlowComponent component, final CommandEvent event) {
        if (indegree <= 1) {
            return false;
        }
        final String waitKey = metaId + event.commandTimestamp + event.getClass().getSimpleName();
        List<CommandEvent> eventsSoFar = CommandEvent.eventsInWaiting.get(waitKey);
        if (eventsSoFar == null) {
            eventsSoFar = new ArrayList<CommandEvent>();
            CommandEvent.eventsInWaiting.put(waitKey, eventsSoFar);
        }
        eventsSoFar.add(event);
        if (eventsSoFar.size() < indegree) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)(component.getMetaName() + "/" + waitKey + " at time " + event.commandTimestamp + " has received " + eventsSoFar.size() + " commands, expecting " + indegree));
            }
            return true;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)(component.getMetaName() + "/" + waitKey + " at time " + event.commandTimestamp + " has received " + eventsSoFar.size() + " which is all of that are expected!"));
        }
        eventsSoFar.clear();
        CommandEvent.eventsInWaiting.remove(waitKey);
        return false;
    }
    
    public IBatch batch() {
        return DefaultBatch.EMPTY_BATCH;
    }
    
    static {
        CommandEvent.logger = Logger.getLogger((Class)CommandEvent.class);
        eventsInWaiting = new HashMap<String, List<CommandEvent>>();
        commandDataObject = new Object();
        commandDataEvent = new Record(CommandEvent.commandDataObject, (Position)null);
    }
}
