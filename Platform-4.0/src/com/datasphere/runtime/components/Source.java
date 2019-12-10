package com.datasphere.runtime.components;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.DisapproveQuiesceException;
import com.datasphere.distribution.HQueue;
import com.datasphere.event.Event;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.intf.EventSink;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.proc.SourceProcess;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.AfterSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.ReportStats;
import com.datasphere.runtime.StreamEventFactory;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorCollector;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Source extends FlowComponent implements Publisher, Runnable, EventSink, Restartable, Compound
{
    private static Logger logger;
    private volatile boolean running;
    private final Channel output;
    private final SourceProcess adapter;
    private final MetaInfo.Source sourceInfo;
    private Subscriber dataSink;
    private Future<?> process;
    private AtomicLong input;
    private SourcePosition currentSourcePosition;
    private boolean currentSourcePositionAtOrAfter;
    private boolean sendPositions;
    private Flow ownerFlow;
    private MetaInfo.Flow curApp;
    private HQueue ehQueue;
    private long lagReportRate;
    private AtomicLong lagReportCounter;
    private AtomicBoolean isItTheFirstLagMarkerSent;
    private boolean lagReportEnabled;
    private boolean isFirstEvent;
    private List<UUID> serversWithDeployedComponent;
    Boolean isMonitorAppSource;
    private Long prevIn;
    Long prevInRate;
    
    public Source(final SourceProcess adapter, final MetaInfo.Source sourceInfo, final BaseServer srv) throws Exception {
        super(srv, sourceInfo);
        this.running = false;
        this.input = new AtomicLong(0L);
        this.curApp = null;
        this.ehQueue = null;
        this.lagReportRate = 10L;
        this.lagReportCounter = new AtomicLong(1L);
        this.isItTheFirstLagMarkerSent = new AtomicBoolean(true);
        this.lagReportEnabled = false;
        this.isFirstEvent = true;
        this.prevIn = null;
        this.prevInRate = null;
        this.adapter = adapter;
        this.sourceInfo = sourceInfo;
        this.output = srv.createSimpleChannel();
        adapter.addEventSink(this);
        final String lagReportEnabledString = System.getProperty("com.dss.lagReportEnabled", "false");
        final String lagReportRateString = System.getProperty("com.dss.lagReportRate", "10");
        if (!lagReportEnabledString.isEmpty()) {
            this.lagReportEnabled = Boolean.parseBoolean(lagReportEnabledString);
        }
        else {
            this.lagReportEnabled = false;
        }
        if (!lagReportRateString.isEmpty()) {
            try {
                this.lagReportRate = Integer.parseInt(lagReportRateString);
            }
            catch (NumberFormatException e) {
                this.lagReportRate = 2147483647L;
                this.lagReportEnabled = false;
            }
        }
        if (Source.logger.isInfoEnabled()) {
            Source.logger.info((Object)(this.getMetaName() + " lagReportEnabled " + this.lagReportEnabled + " lagReportRate" + this.lagReportRate));
        }
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSink = flow.getSubscriber(this.sourceInfo.outputStream);
    }
    
    public Stream connectStream() throws Exception {
        final Stream stream = this.srv().getStream(this.sourceInfo.outputStream, this.ownerFlow);
        if (this.curApp != null) {
            final boolean isRecoveryEnabled = this.curApp.recoveryType == 2;
            stream.setRecoveryEnabled(isRecoveryEnabled);
        }
        return (Stream)(this.dataSink = stream);
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void close() throws Exception {
        this.stop();
        this.output.close();
    }
    
    @Override
    public void run() {
        while (this.running) {
            try {
                if (!this.injectedCommandEvents.isEmpty()) {
                    final CommandEvent commandEvent = this.injectedCommandEvents.peek();
                    if (commandEvent != null) {
                        if (Logger.getLogger("Commands").isDebugEnabled()) {
                            Logger.getLogger("Commands").debug((Object)("Source " + this.getMetaName() + " is now handling " + commandEvent.getClass() + "..."));
                        }
                        commandEvent.performCommand(this);
                    }
                    this.injectedCommandEvents.poll();
                    continue;
                }
            }
            catch (Exception e) {
                Source.logger.error((Object)"Error while processing a Command", (Throwable)e);
            }
            if (!this.paused) {
                if (this.isFlowInError()) {
                    return;
                }
                try {
                    this.adapter.receive(0, null);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (!this.running) {
                        Source.logger.info((Object)(Thread.currentThread().getName() + "running " + this.getMetaFullName() + " interrupted as part of STOP routine"));
                    }
                    else {
                        this.handleException(ie);
                    }
                }
                catch (Exception ex) {
                    this.handleException(ex);
                }
            }
        }
    }
    
    private void handleException(final Exception ex) {
        Source.logger.error((Object)("Exception thrown by Source Adapter " + this.getMetaName()), (Throwable)ex);
        this.notifyAppMgr(EntityType.SOURCE, this.getMetaName(), this.getMetaID(), ex, "source run", new Object[0]);
        final ExceptionType exceptionType = ExceptionType.getExceptionType(ex);
        if (exceptionType.name().equalsIgnoreCase(ExceptionType.UnknownException.name())) {
            try {
                this.stop();
            }
            catch (Exception e) {
                Source.logger.error((Object)e, (Throwable)e);
            }
        }
    }
    
    @Override
    public void receive(final int channel, final Event event) throws Exception {
        this.setProcessThread();
        final TaskEvent taskEvent = (TaskEvent)StreamEventFactory.createStreamEvent(event);
        if (!this.isMonitorAppSource() && this.lagReportEnabled && this.lagReportCounter.decrementAndGet() == 0L) {
            taskEvent.setLagMarker(new LagMarker(this.getTopLevelFlow().getMetaID()));
            this.recordLagMarker(taskEvent.getLagMarker());
            taskEvent.setLagRecord(true);
            this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)taskEvent);
            if (this.isItTheFirstLagMarkerSent.getAndSet(false) && Source.logger.isInfoEnabled()) {
                Source.logger.info((Object)("Lag Marker is sent first time from the component " + this.getMetaFullName()));
            }
            this.lagReportCounter.set(this.lagReportRate);
        }
        this.output.publish((ITaskEvent)taskEvent);
        this.input.getAndIncrement();
        this.updateSessionStats(event);
    }
    
    @Override
    public void addSessionToReport(final AuthToken token) {
        final ReportStats.SourceReportStats stats = new ReportStats.SourceReportStats(this.getMetaInfo());
        this.compStats.put(token, stats);
    }
    
    @Override
    public ReportStats.BaseReportStats removeSessionToReport(final AuthToken token, final boolean computeRetVal) {
        final ReportStats.SourceReportStats stats = (ReportStats.SourceReportStats)this.compStats.remove(token);
        if (stats == null || !computeRetVal) {
            return null;
        }
        if (stats.getFirstEvent() != null) {
            stats.setFirstEventStr(stats.getFirstEvent().toString());
            stats.setFirstEvent(null);
        }
        if (stats.getLastEvent() != null) {
            stats.setLastEventStr(stats.getLastEvent().toString());
            stats.setLastEvent(null);
        }
        return stats;
    }
    
    public void updateSessionStats(final Event e) {
        for (final ReportStats.BaseReportStats stats : this.compStats.values()) {
            final ReportStats.SourceReportStats srcStats = (ReportStats.SourceReportStats)stats;
            final long eventTS = System.currentTimeMillis();
            long evtsSeen = srcStats.getEventsSeen();
            srcStats.setEventsSeen(++evtsSeen);
            if (srcStats.getFirstEvent() == null) {
                srcStats.setFirstEvent(e);
                srcStats.setFirstEventTS(eventTS);
            }
            srcStats.setLastEvent(e);
            srcStats.setLastEventTS(eventTS);
        }
    }
    
    @Override
    public void receive(final int channel, final Event event, final Position pos) throws Exception {
        if (this.isFirstEvent && pos != null) {
            this.isFirstEvent = false;
            if (this.currentSourcePosition != null && this.currentSourcePositionAtOrAfter) {
                final SourcePosition sp = pos.getLowSourcePositionForComponent(this.getMetaID());
                if (sp != null && sp.compareTo(this.currentSourcePosition) == 0) {
                    return;
                }
            }
        }
        this.setProcessThread();
        final TaskEvent taskEvent = (TaskEvent)StreamEventFactory.createStreamEvent(event, pos);
        if (!this.isMonitorAppSource() && this.lagReportEnabled && this.lagReportCounter.decrementAndGet() == 0L) {
            taskEvent.setLagMarker(new LagMarker(this.getTopLevelFlow().getMetaID()));
            this.recordLagMarker(taskEvent.getLagMarker());
            taskEvent.setLagRecord(true);
            this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)taskEvent);
            if (this.isItTheFirstLagMarkerSent.getAndSet(false) && Source.logger.isInfoEnabled()) {
                Source.logger.info((Object)("Lag Marker is sent first time from the component " + this.getMetaFullName()));
            }
            this.lagReportCounter.set(this.lagReportRate);
        }
        this.output.publish((ITaskEvent)taskEvent);
        this.input.getAndIncrement();
        this.updateSessionStats(event);
    }
    
    @Override
    public void receive(final ITaskEvent batch) throws Exception {
        this.setProcessThread();
        if (!this.isMonitorAppSource()) {
            final TaskEvent taskEvent = (TaskEvent)batch;
            if (this.lagReportEnabled && this.lagReportCounter.decrementAndGet() == 0L) {
                taskEvent.setLagMarker(new LagMarker(this.getTopLevelFlow().getMetaID()));
                this.recordLagMarker(taskEvent.getLagMarker());
                taskEvent.setLagRecord(true);
                this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)taskEvent);
                if (this.isItTheFirstLagMarkerSent.getAndSet(false) && Source.logger.isInfoEnabled()) {
                    Source.logger.info((Object)("Lag Marker is sent first time from the component " + this.getMetaFullName()));
                }
                this.lagReportCounter.set(this.lagReportRate);
            }
        }
        this.output.publish(batch);
        this.input.getAndIncrement();
    }
    
    @Override
    public synchronized void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        this.paused = false;
        if (!this.injectedCommandEvents.isEmpty()) {
            for (final CommandEvent commandEvent : this.injectedCommandEvents) {
                Source.logger.error((Object)("Source on start " + this.getMetaName() + " is abandoning injected Command: " + commandEvent));
            }
            this.injectedCommandEvents.clear();
        }
        this.isFirstEvent = true;
        String distId = BaseServer.getServerName();
        if (this.ownerFlow != null) {
            distId = this.ownerFlow.getDistributionId();
        }
        final ClassLoader originalCl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.adapter.getClass().getClassLoader());
        if (this.adapter.isAdapterShardable()) {
            this.adapter.init(this.sourceInfo.properties, this.sourceInfo.parserProperties, this.getMetaID(), distId, this.currentSourcePosition, this.sendPositions, this.ownerFlow, this.serversWithDeployedComponent);
        }
        else {
            this.adapter.init(this.sourceInfo.properties, this.sourceInfo.parserProperties, this.getMetaID(), distId, this.currentSourcePosition, this.sendPositions, this.ownerFlow);
        }
        Thread.currentThread().setContextClassLoader(originalCl);
        this.srv().subscribe(this, this.dataSink);
        final ExecutorService pool = this.srv().getThreadPool();
        this.process = pool.submit(this);
    }
    
    @Override
    public void injectCommandEvent(final CommandEvent commandEvent) throws Exception {
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Source " + this.getMetaName() + " is injecting " + commandEvent.getClass() + "  "));
        }
        if (this.running) {
            this.injectedCommandEvents.add(commandEvent);
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Source " + this.getMetaName() + " injected command event " + commandEvent));
        }
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        if (!this.isMonitorAppSource() && this.lagReportEnabled && event.canBeLagEvent()) {
            final TaskEvent taskEvent = (TaskEvent)event;
            if (this.lagReportCounter.decrementAndGet() == 0L) {
                taskEvent.setLagMarker(new LagMarker(this.getTopLevelFlow().getMetaID()));
                this.recordLagMarker(taskEvent.getLagMarker());
                taskEvent.setLagRecord(true);
                this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)taskEvent);
                if (this.isItTheFirstLagMarkerSent.getAndSet(false) && Source.logger.isInfoEnabled()) {
                    Source.logger.info((Object)("Lag Marker is sent first time from the component " + this.getMetaFullName()));
                }
                this.lagReportCounter.set(this.lagReportRate);
            }
        }
        this.output.publish(event);
    }
    
    @Override
    public synchronized void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.resetProcessThread();
        this.running = false;
        this.srv().unsubscribe(this, this.dataSink);
        if (this.adapter != null) {
            this.adapter.close();
        }
        if (this.process != null) {
            this.process.cancel(true);
            this.process = null;
        }
        if (!this.injectedCommandEvents.isEmpty()) {
            for (final CommandEvent commandEvent : this.injectedCommandEvents) {
                Source.logger.error((Object)("Source on stop " + this.getMetaName() + " is abandoning injected Command: " + commandEvent));
            }
            this.injectedCommandEvents.clear();
        }
        final long time = System.currentTimeMillis();
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.INPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.SOURCE_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.RATE, Long.valueOf(0L), Long.valueOf(time)));
    }
    
    public void disconnect() throws Exception {
        this.srv().unsubscribe(this, this.dataSink);
    }
    
    boolean isMonitorAppSource() {
        if (this.isMonitorAppSource != null) {
            return this.isMonitorAppSource;
        }
        if (!this.getMetaInfo().getName().equals("MonitoringSource1")) {
            this.isMonitorAppSource = false;
        }
        else {
            this.isMonitorAppSource = true;
        }
        return this.isMonitorAppSource;
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        this.adapter.publishMonitorEvents(monEvs);
        final Long in = this.input.get();
        if (this.prevIn == null) {
            monEvs.add(new MonitorEvent(monEvs.getServerID(), monEvs.getEntityID(), MonitorEvent.Type.INPUT, Long.valueOf(0L), Long.valueOf(monEvs.getTimeStamp() - 1L)));
            monEvs.add(new MonitorEvent(monEvs.getServerID(), monEvs.getEntityID(), MonitorEvent.Type.SOURCE_INPUT, Long.valueOf(0L), Long.valueOf(monEvs.getTimeStamp() - 1L)));
        }
        if (!in.equals(this.prevIn)) {
            monEvs.add(MonitorEvent.Type.INPUT, in, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.SOURCE_INPUT, in, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
        }
        final Long rate = monEvs.getRate(in, this.prevIn);
        if (rate != null && !Objects.equals(rate, this.prevInRate)) {
            monEvs.add(MonitorEvent.Type.INPUT_RATE, rate, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.SOURCE_RATE, rate, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.RATE, rate, MonitorEvent.Operation.SUM);
        }
        this.prevIn = in;
        this.prevInRate = rate;
    }
    
    @Override
    public UUID getNodeID() {
        return HazelcastSingleton.getNodeId();
    }
    
    public SourceProcess getAdapter() {
        return this.adapter;
    }
    
    public void setPosition(final SourcePosition sourcePosition) {
        if (sourcePosition instanceof AfterSourcePosition) {
            this.currentSourcePositionAtOrAfter = true;
            this.currentSourcePosition = ((AfterSourcePosition)sourcePosition).getSourcePosition();
        }
        else {
            this.currentSourcePosition = sourcePosition;
        }
    }
    
    public void setSendPositions(final boolean sendPositions) {
        this.sendPositions = sendPositions;
    }
    
    public void setOwnerFlow(final Flow flow) {
        this.ownerFlow = flow;
    }
    
    public boolean requiresPartitionedSourcePosition() {
        return this.adapter.requiresPartitionedSourcePosition();
    }
    
    @Override
    public void setPaused(final boolean b) {
        this.paused = b;
    }
    
    @Override
    public boolean approveQuiesce() throws DisapproveQuiesceException {
        return this.adapter.approveQuiesce();
    }
    
    public void serversWithDeployedComponent(final List<UUID> servers) {
        this.serversWithDeployedComponent = servers;
    }
    
    @Override
    public Position getCheckpoint() {
        final Position checkpoint = this.adapter.getCheckpoint();
        if (checkpoint == null) {
            return null;
        }
        final PathManager pm = new PathManager(checkpoint);
        pm.setAtOrAfter("^");
        return pm.toPosition();
    }
    
    static {
        Source.logger = Logger.getLogger((Class)Source.class);
    }
}
