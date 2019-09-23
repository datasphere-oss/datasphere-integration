package com.datasphere.runtime.components;

import org.apache.log4j.*;
import java.text.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.event.*;
import com.datasphere.messaging.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.proc.events.commands.*;
import com.datasphere.runtime.channels.*;
import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;

public class MultiComponent extends FlowComponent implements Compound, Restartable, PubSub
{
    int numInstances;
    Map<Integer, TargetTask> instances;
    Map<Integer, InterThreadComm> commMap;
    private volatile boolean running;
    private FlowComponent oneComponentInstance;
    int atint;
    private static Logger logger;
    DecimalFormat decimalFormat;
    long inputCount;
    
    public MultiComponent(final BaseServer srv, final MetaInfo.MetaObject info) {
        super(srv, info);
        this.running = false;
        this.atint = 0;
        this.decimalFormat = new DecimalFormat("0.00");
        this.inputCount = 0L;
    }
    
    @Override
    public boolean canReceiveDataSynchronously() {
        return true;
    }
    
    @Override
    public void close() throws Exception {
        for (final TargetTask task : this.instances.values()) {
            task.flowComponent.close();
        }
        this.instances.clear();
        this.commMap.clear();
    }
    
    @Override
    public void start() throws Exception {
        this.srv().subscribe(this.dataSource, this);
        for (final Map.Entry<Integer, TargetTask> entry : this.instances.entrySet()) {
            if (entry.getValue().flowComponent instanceof Restartable) {
                final int queueSize = Integer.parseInt(System.getProperty("com.datasphere.config.mc.queuesize" + this.getMetaFullName(), "65536"));
                final InterThreadComm threadComm = new HDisruptor<Object>(queueSize, entry.getKey(), entry.getValue(), System.getProperty("com.datasphere.config.waitStrategy." + this.getMetaFullName(), HDisruptor.WAITSTRATEGY.yield.name()), this.getMetaFullName());
                this.commMap.put(entry.getKey(), threadComm);
                ((Restartable)entry.getValue().flowComponent).start();
            }
        }
    }
    
    @Override
    public void stop() throws Exception {
        this.srv().unsubscribe(this.dataSource, this);
        for (final TargetTask task : this.instances.values()) {
            task.flowComponent.stop();
        }
        for (final InterThreadComm threadComm : this.commMap.values()) {
            threadComm.stop();
        }
    }
    
    @Override
    public void flush() throws Exception {
        for (final TargetTask task : this.instances.values()) {
            task.flowComponent.flush();
        }
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection events) {
        final Map<MonitorEvent.Type, MonitorEvent> result = new HashMap<MonitorEvent.Type, MonitorEvent>();
        if (this.instances == null) {
            return;
        }
        for (final TargetTask task : this.instances.values()) {
            if (task == null) {
                continue;
            }
            task.addSpecificMonitorEvents(events);
            events.getEvents().clear();
            for (final Map.Entry<MonitorEvent.Type, MonitorEvent> monevents : task.monMap.entrySet()) {
                final MonitorEvent monitorEvent = monevents.getValue();
                if (monitorEvent.type.equals(MonitorEvent.Type.INPUT)) {
                    continue;
                }
                MonitorEvent mapEvent = result.get(monitorEvent.type);
                if (mapEvent == null) {
                    mapEvent = new MonitorEvent(monitorEvent);
                    result.put(mapEvent.type, mapEvent);
                    if (!Logger.getLogger("MCmonitor").isDebugEnabled()) {
                        continue;
                    }
                    Logger.getLogger("MCmonitor").debug((Object)(("Instance " + task.instanceNumber + " : Type - " + monitorEvent.type + ":" + monitorEvent.valueString != null) ? monitorEvent.valueString : monitorEvent.valueLong));
                }
                else {
                    if (Logger.getLogger("MCmonitor").isDebugEnabled()) {
                        Logger.getLogger("MCmonitor").debug((Object)(("Instance " + task.instanceNumber + " : Type - " + monitorEvent.type + ":" + monitorEvent.valueString != null) ? monitorEvent.valueString : monitorEvent.valueLong));
                    }
                    switch (monitorEvent.operation) {
                        case SUM: {
                            this.covertAndSet(mapEvent, monitorEvent);
                            continue;
                        }
                        case AVG: {
                            this.covertAndSet(mapEvent, monitorEvent);
                            continue;
                        }
                        case MAX: {
                            mapEvent.valueLong = ((mapEvent.valueLong > monitorEvent.valueLong) ? mapEvent.valueLong : monitorEvent.valueLong);
                        }
                        case MIN: {
                            mapEvent.valueLong = ((mapEvent.valueLong < monitorEvent.valueLong) ? mapEvent.valueLong : monitorEvent.valueLong);
                            continue;
                        }
                    }
                }
            }
            events.getEvents().clear();
        }
        if (Logger.getLogger("MCmonitor").isDebugEnabled()) {
            for (final Map.Entry<MonitorEvent.Type, MonitorEvent> entry : result.entrySet()) {
                Logger.getLogger("MCmonitor").debug((Object)(("Type - " + entry.getKey() + ":" + entry.getValue().valueString != null) ? entry.getValue().valueString : (entry.getValue().valueLong + ", Operation-" + entry.getValue().operation)));
            }
        }
        events.getEvents().addAll(result.values());
        events.add(MonitorEvent.Type.INPUT, Long.valueOf(this.inputCount));
        if (Logger.getLogger("MCInstanceStats").isDebugEnabled()) {
            for (final TargetTask task : this.instances.values()) {
                Logger.getLogger("MCInstanceStats").debug((Object)(Server.getServer().getServerID() + "-Target-" + task.instanceNumber + " events processed : " + task.count));
            }
        }
    }
    
    void covertAndSet(final MonitorEvent mapEvent, final MonitorEvent monitorEvent) {
        if (monitorEvent.valueString != null) {
            final Double d = Double.parseDouble(mapEvent.valueString) + Double.parseDouble(monitorEvent.valueString);
            mapEvent.valueString = this.decimalFormat.format(d);
        }
        else {
            mapEvent.valueLong += monitorEvent.valueLong;
        }
    }
    
    public void addComponent(final FlowComponent tg) {
        if (this.oneComponentInstance == null) {
            this.oneComponentInstance = tg;
        }
        this.instances.put(this.atint, new TargetTask(tg, this.atint));
        ++this.atint;
    }
    
    public void setNumInstances(final int parallelismFactor) {
        this.numInstances = parallelismFactor;
        this.instances = new HashMap<Integer, TargetTask>(parallelismFactor);
        this.commMap = new HashMap<Integer, InterThreadComm>(parallelismFactor);
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSource = flow.getPublisher(((MetaInfo.Target)this.getMetaInfo()).inputStream);
        for (final TargetTask task : this.instances.values()) {
            if (task.flowComponent instanceof Target) {
                ((Target)task.flowComponent).setPublisher(this);
            }
        }
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        this.setProcessThread();
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        if (this.oneComponentInstance instanceof Target) {
            for (final DARecord DARecord : event.batch()) {
                final int partitionId = Integer.parseInt(((Target)this.oneComponentInstance).getAdapter().getDistributionId((Event)DARecord.data));
                final int id = partitionId % this.numInstances;
                final InterThreadComm threadComm = this.commMap.get(id);
                if (threadComm != null) {
                    final TargetTask task = this.instances.get(id);
                    if (task == null) {
                        continue;
                    }
                    threadComm.put(DARecord, null, 0, task);
                    ++this.inputCount;
                }
            }
        }
    }
    
    @Override
    public Channel getChannel() {
        return new SimpleChannel();
    }
    
    @Override
    public Position getCheckpoint() {
        if (this.instances == null) {
            return null;
        }
        final List<Path> allPaths = new ArrayList<Path>();
        for (final TargetTask task : this.instances.values()) {
            if (task == null) {
                continue;
            }
            final Position checkpoint = task.flowComponent.getCheckpoint();
            if (Logger.getLogger("MCcheckpoint").isDebugEnabled()) {
                Logger.getLogger("MCcheckpoint").debug((Object)("Target-" + task.instanceNumber + " checkpoint size: " + checkpoint.values().size() + "\n" + checkpoint));
            }
            if (checkpoint == null || checkpoint.isEmpty()) {
                continue;
            }
            allPaths.addAll(checkpoint.values());
        }
        final Position finalPosition = new Position((List)allPaths);
        if (Logger.getLogger("MCcheckpoint").isDebugEnabled()) {
            Logger.getLogger("MCcheckpoint").debug((Object)("Final position after merging all Target's has " + allPaths.size() + " paths. \n Position: " + finalPosition));
        }
        return finalPosition;
    }
    
    public Position getEndpointCheckpoint() {
        if (this.instances == null) {
            return null;
        }
        final List<Path> allPaths = new ArrayList<Path>();
        for (final TargetTask task : this.instances.values()) {
            final Position checkpoint = ((Target)task.flowComponent).getEndpointCheckpoint();
            if (Logger.getLogger("MCcheckpoint").isDebugEnabled()) {
                Logger.getLogger("MCcheckpoint").debug((Object)("Target-" + task.instanceNumber + " end checkpoint size: " + checkpoint.values().size() + "\n" + checkpoint));
            }
            if (checkpoint != null && !checkpoint.isEmpty()) {
                allPaths.addAll(checkpoint.values());
            }
        }
        final Position finalPosition = new Position((List)allPaths);
        if (Logger.getLogger("MCcheckpoint").isDebugEnabled()) {
            Logger.getLogger("MCcheckpoint").debug((Object)("Final position after merging all Target's has " + allPaths.size() + " paths. \n End position: " + finalPosition));
        }
        return finalPosition;
    }
    
    static {
        MultiComponent.logger = Logger.getLogger((Class)MultiComponent.class);
    }
    
    class TargetTask implements ChannelEventHandler
    {
        FlowComponent flowComponent;
        int instanceNumber;
        long count;
        Map<MonitorEvent.Type, MonitorEvent> monMap;
        
        TargetTask(final FlowComponent flowComponent, final int instanceNumber) {
            this.monMap = new HashMap<MonitorEvent.Type, MonitorEvent>();
            this.flowComponent = flowComponent;
            this.instanceNumber = instanceNumber;
        }
        
        @Override
        public void sendEvent(final EventContainer event, final int sendQueueId, final LagMarker lagMarker) throws InterruptedException {
            try {
                ((Subscriber)this.flowComponent).receive(null, (ITaskEvent)new DefaultTaskEvent((DARecord)event.getEvent()));
                ++this.count;
            }
            catch (InterruptedException e2) {
                Thread.currentThread().interrupt();
                MultiComponent.logger.warn((Object)"Interrupted while running will exit!");
            }
            catch (Exception e) {
                MultiComponent.this.notifyAppMgr(MultiComponent.this.getMetaType(), MultiComponent.this.getMetaFullName(), MultiComponent.this.getMetaID(), e, "", new Object[0]);
            }
        }
        
        public void addSpecificMonitorEvents(final MonitorEventsCollection events) {
            this.flowComponent.addSpecificMonitorEvents(events);
            for (final MonitorEvent monitorEvent3 : events.getEvents()) {
                this.monMap.merge(monitorEvent3.type, monitorEvent3, (monitorEvent1, monitorEvent2) -> {
                    if (monitorEvent1.valueString != null) {
                        if (monitorEvent1.valueString != monitorEvent2.valueString) {
                            return monitorEvent2;
                        }
                        else {
                            return monitorEvent1;
                        }
                    }
                    else if (monitorEvent1.valueLong != monitorEvent2.valueLong) {
                        return monitorEvent2;
                    }
                    else {
                        return monitorEvent1;
                    }
                });
            }
        }
    }
}
