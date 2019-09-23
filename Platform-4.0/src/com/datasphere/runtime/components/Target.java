package com.datasphere.runtime.components;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.proc.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.*;
import java.util.*;
import com.datasphere.proc.events.commands.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;
import com.datasphere.utility.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

public class Target extends FlowComponent implements Subscriber, Restartable, Compound, ReceiptCallback
{
    private static Logger logger;
    private final MetaInfo.Target targetInfo;
    private final BaseProcess adapter;
    private Publisher dataSource;
    private volatile boolean running;
    private volatile long eventsInput;
    private AtomicLong eventsAcked;
    private volatile long bytesWritten;
    private volatile long latency;
    private volatile long maxLatency;
    private boolean useDeliveryCallback;
    private PathManager memoryCheckpoint;
    private PathManager waitCheckpoint;
    LagMarker lastLagObserved;
    Boolean lagReportEnabled;
    ConcurrentHashMap<String, LagMarker> allPaths;
    private Long prevIn;
    private Long prevAck;
    private Long prevOutput;
    private Long prevRate;
    private Long prevAckRate;
    private Long prevBytesWritten;
    private Long prevBytesWriteRate;
    private double bytesWriteRateDouble;
    long lastTimePrinted;
    
    public Target(final BaseProcess adapter, final MetaInfo.Target targetInfo, final BaseServer srv) throws Exception {
        super(srv, targetInfo);
        this.running = false;
        this.eventsInput = 0L;
        this.eventsAcked = new AtomicLong(0L);
        this.bytesWritten = 0L;
        this.latency = 0L;
        this.maxLatency = 0L;
        this.useDeliveryCallback = false;
        this.memoryCheckpoint = new PathManager();
        this.waitCheckpoint = new PathManager();
        this.lastLagObserved = null;
        this.lagReportEnabled = Boolean.parseBoolean(System.getProperty("com.striim.lagReportEnabled", "false"));
        this.allPaths = new ConcurrentHashMap<String, LagMarker>();
        this.prevIn = null;
        this.prevAck = null;
        this.prevOutput = null;
        this.prevRate = null;
        this.prevAckRate = null;
        this.prevBytesWritten = null;
        this.prevBytesWriteRate = null;
        this.bytesWriteRateDouble = 0.0;
        this.lastTimePrinted = 0L;
        this.adapter = adapter;
        this.targetInfo = targetInfo;
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSource = flow.getPublisher(this.targetInfo.inputStream);
    }
    
    @Override
    public void addSessionToReport(final AuthToken token) {
        final ReportStats.TargetReportStats stats = new ReportStats.TargetReportStats(this.getMetaInfo());
        this.compStats.put(token, stats);
    }
    
    @Override
    public ReportStats.BaseReportStats removeSessionToReport(final AuthToken token, final boolean computeRetVal) {
        final ReportStats.TargetReportStats stats = (ReportStats.TargetReportStats)this.compStats.remove(token);
        if (stats == null || !computeRetVal) {
            return null;
        }
        if (stats.getLastEvent() != null) {
            stats.setLastEventStr(stats.getLastEvent().toString());
            stats.setLastEvent(null);
        }
        return stats;
    }
    
    public void updateSessionStats(final Event e) {
        for (final ReportStats.BaseReportStats stats : this.compStats.values()) {
            final ReportStats.TargetReportStats tgtStats = (ReportStats.TargetReportStats)stats;
            final long eventTS = System.currentTimeMillis();
            long evtsSeen = tgtStats.getEventsOutput();
            tgtStats.setEventsOutput(++evtsSeen);
            tgtStats.setLastEventTS(eventTS);
            tgtStats.setLastEvent(e);
        }
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (this.lagReportEnabled && this.shouldMarkerBePassedAlong(event)) {
            this.recordLagMarker(((TaskEvent)event).getLagMarker());
            this.lastLagObserved = ((TaskEvent)event).getLagMarker().copy();
            this.lagRecordInfo(this.getMetaInfo(), this.lastLagObserved);
            this.allPaths.put(this.lastLagObserved.key(), this.lastLagObserved);
        }
        try {
            this.setProcessThread();
            if (event instanceof CommandEvent) {
                ((CommandEvent)event).performCommand(this);
                return;
            }
            for (final DARecord ev : event.batch()) {
                if (!this.running) {
                    if (Logger.getLogger("Recovery").isInfoEnabled()) {
                        Logger.getLogger("Recovery").info((Object)(this.getMetaName() + " is stopped but received a batch of " + event.batch().size() + " events"));
                    }
                    return;
                }
                if (this.isFlowInError()) {
                    if (Logger.getLogger("Recovery").isInfoEnabled()) {
                        Logger.getLogger("Recovery").info((Object)(this.getMetaName() + " is in error but received a batch of " + event.batch().size() + " events"));
                    }
                    return;
                }
                if (ev.position == null) {
                    final Event actualEvent = (Event)ev.data;
                    this.adapter.receive(0, actualEvent);
                    ++this.eventsInput;
                }
                else {
                    final String distId = this.adapter.getDistributionId((Event)ev.data);
                    final Position dataPosition = ev.position.createAugmentedPosition(this.getMetaID(), distId);
                    if (this.waitCheckpoint != null) {
                        if (this.waitCheckpoint.equalsOrExceeds(dataPosition)) {
                            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                                Logger.getLogger("Recovery").debug((Object)(this.getMetaName() + " dropping duplicate: " + ev));
                                continue;
                            }
                            continue;
                        }
                        else {
                            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                                Logger.getLogger("Recovery").debug((Object)(this.getMetaName() + " accepting: " + ev));
                            }
                            this.waitCheckpoint.removePaths(dataPosition);
                            if (this.waitCheckpoint.isEmpty()) {
                                this.waitCheckpoint = null;
                            }
                        }
                    }
                    if (this.useDeliveryCallback) {
                        this.memoryCheckpoint.mergeLowerPositions(dataPosition);
                    }
                    if (Logger.getLogger("Recovery").isDebugEnabled()) {
                        Logger.getLogger("Recovery").debug((Object)(this.getMetaName() + " received event " + event));
                    }
                    final Event actualEvent2 = (Event)ev.data;
                    this.adapter.receive(0, actualEvent2, dataPosition);
                    ++this.eventsInput;
                    this.updateSessionStats(actualEvent2);
                }
            }
        }
        catch (Exception e) {
            boolean isInterrupted = false;
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof InterruptedException) {
                    isInterrupted = true;
                    break;
                }
            }
            if (isInterrupted) {
                Thread.currentThread().interrupt();
                throw e;
            }
            Target.logger.error((Object)("Got exception " + e.getMessage()), (Throwable)e);
            this.notifyAppMgr(EntityType.TARGET, this.getMetaName(), this.getMetaID(), e, "target receive", event);
            throw new AdapterException("Exception in Target Adapter " + this.getMetaName(), (Throwable)e);
        }
    }
    
    public BaseProcess getAdapter() {
        return this.adapter;
    }
    
    @Override
    public void flush() throws Exception {
        this.adapter.flush();
    }
    
    @Override
    public void close() throws Exception {
        this.resetProcessThread();
    }
    
    @Override
    public synchronized void start() throws Exception {
        if (this.running) {
            return;
        }
        if (!this.useDeliveryCallback && this.adapter instanceof Acknowledgeable) {
            this.adapter.setReceiptCallback(this);
            this.useDeliveryCallback = true;
        }
        this.adapter.setRecoveryEnabled(this.recoveryIsEnabled());
        if (Target.logger.isDebugEnabled()) {
            Target.logger.debug((Object)("starting target : " + this.targetInfo.name));
        }
        try {
            final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.adapter.getClass().getClassLoader());
            this.targetInfo.properties.put("TargetUUID", this.getMetaID());
            this.adapter.init(this.targetInfo.properties, this.targetInfo.formatterProperties, this.targetInfo.inputStream, null);
            Thread.currentThread().setContextClassLoader(origialCl);
        }
        catch (Exception e) {
            throw new AdapterException("Initialization exception in Target Adapter " + this.getMetaName(), (Throwable)e);
        }
        if (this.recoveryIsEnabled()) {
            final Position targetChkPnt = Utility.updateUuids(this.adapter.getWaitPosition());
            if (targetChkPnt != null) {
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("Setting Target " + this.getMetaName() + " waitCheckpoint to "));
                    Utility.prettyPrint(targetChkPnt);
                }
                this.waitCheckpoint = new PathManager(targetChkPnt);
            }
        }
        this.adapter.startWorker();
        this.srv().subscribe(this.dataSource, this);
        this.running = true;
    }
    
    @Override
    public synchronized void stop() throws Exception {
        if (!this.running) {
            return;
        }
        if (Target.logger.isDebugEnabled()) {
            Target.logger.debug((Object)("stopping target : " + this.targetInfo.name));
        }
        this.srv().unsubscribe(this.dataSource, this);
        this.adapter.stopWorker();
        this.adapter.close();
        this.running = false;
        final long time = System.currentTimeMillis();
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.INPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.SOURCE_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.RATE, Long.valueOf(0L), Long.valueOf(time)));
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        this.adapter.publishMonitorEvents(monEvs);
        Long latestActivity = null;
        if (this.lastLagObserved != null) {
            final StringBuilder stringBuilder = new StringBuilder();
            for (final LagMarker lagMarker : this.allPaths.values()) {
                stringBuilder.append(lagMarker.toString() + "\n");
            }
            stringBuilder.append("\n");
            monEvs.add(MonitorEvent.Type.LAG_REPORT, stringBuilder.toString());
            monEvs.add(MonitorEvent.Type.LAG_RATE, this.lastLagObserved.calculateTotalLag());
            final ObjectMapper objectMapper = ObjectMapperFactory.newInstance();
            final ArrayNode fieldArray = objectMapper.createArrayNode();
            for (final LagMarker lagMarker2 : this.allPaths.values()) {
                fieldArray.add(lagMarker2.toJSON());
            }
            monEvs.add(MonitorEvent.Type.LAG_REPORT_JSON, fieldArray.toString());
        }
        final Long in = this.eventsInput;
        if (!in.equals(this.prevIn)) {
            monEvs.add(MonitorEvent.Type.INPUT, in, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.OUTPUT, in, MonitorEvent.Operation.SUM);
            latestActivity = monEvs.getTimeStamp();
        }
        final Long ack = this.eventsAcked.get();
        if (ack > 0L && !ack.equals(this.prevAck)) {
            monEvs.add(MonitorEvent.Type.TARGET_ACKED, ack, MonitorEvent.Operation.SUM);
            latestActivity = monEvs.getTimeStamp();
        }
        final Long output = this.useDeliveryCallback ? ack : in;
        if (!output.equals(this.prevOutput)) {
            monEvs.add(MonitorEvent.Type.TARGET_OUTPUT, output, MonitorEvent.Operation.SUM);
            latestActivity = monEvs.getTimeStamp();
        }
        final Long rate = monEvs.getRate(output, this.prevOutput);
        if (rate != null && !rate.equals(this.prevRate)) {
            monEvs.add(MonitorEvent.Type.INPUT_RATE, rate, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.RATE, rate, MonitorEvent.Operation.SUM);
            monEvs.add(MonitorEvent.Type.TARGET_RATE, rate, MonitorEvent.Operation.SUM);
            latestActivity = monEvs.getTimeStamp();
        }
        final Long bytes = this.bytesWritten;
        final Long bytesWriteRate = monEvs.getRate(this.bytesWritten, this.prevBytesWritten);
        if (bytesWriteRate != null && !bytesWriteRate.equals(this.prevBytesWriteRate)) {
            this.bytesWriteRateDouble = bytesWriteRate / 1048576.0;
            monEvs.add(MonitorEvent.Type.WRITE_BYTES_RATE_MB_per_SEC, Double.toString(this.bytesWriteRateDouble), MonitorEvent.Operation.SUM);
            latestActivity = monEvs.getTimeStamp();
        }
        if (this.latency > 0L) {
            final Long l = this.latency;
            final Double avgLatency = l / (ack * 1.0);
            monEvs.add(MonitorEvent.Type.AVG_LATENCY, avgLatency.toString(), MonitorEvent.Operation.AVG);
            monEvs.add(MonitorEvent.Type.MAX_LATENCY, Long.valueOf(this.maxLatency), MonitorEvent.Operation.MAX);
            latestActivity = monEvs.getTimeStamp();
        }
        if (latestActivity != null) {
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, latestActivity, MonitorEvent.Operation.MAX);
        }
        this.prevIn = in;
        this.prevAck = ack;
        this.prevOutput = output;
        this.prevRate = rate;
        this.prevBytesWritten = bytes;
        this.prevBytesWriteRate = bytesWriteRate;
    }
    
    @Override
    public String toString() {
        return this.getMetaUri() + " - " + this.getMetaID() + " RUNTIME";
    }
    
    @Override
    public Position getCheckpoint() {
        return this.memoryCheckpoint.toPosition();
    }
    
    public Position getEndpointCheckpoint() {
        try {
            return (this.waitCheckpoint == null) ? this.adapter.getWaitPosition() : this.waitCheckpoint.toPosition();
        }
        catch (Exception e) {
            Target.logger.error((Object)e);
            return null;
        }
    }
    
    public void onDeploy() throws Exception {
        this.adapter.onDeploy(this.targetInfo.properties, this.targetInfo.formatterProperties, this.targetInfo.parallelismProperties, this.targetInfo.inputStream);
    }
    
    @Override
    public void ack(final int count, final Position eventPosition) {
        if (count < 1) {
            Target.logger.warn((Object)("Target " + this.getMetaName() + " received ack count of " + count + " and position " + eventPosition + " which cannot be processed because the count is less than 1."));
            return;
        }
        this.memoryCheckpoint.mergeHigherPositions(eventPosition);
        this.memoryCheckpoint.removeEqualPaths(eventPosition);
        this.eventsAcked.getAndAdd(count);
        if (Target.logger.isDebugEnabled()) {
            Target.logger.debug((Object)("Target " + this.getMetaName() + " received and processed ack count of " + count + " and position " + eventPosition));
        }
    }
    
    @Override
    public void notifyException(final Exception exception, final Event event) {
        if (event != null) {
            this.notifyAppMgr(EntityType.TARGET, this.getMetaName(), this.getMetaID(), exception, "target notify exception", event);
        }
        else {
            this.notifyAppMgr(EntityType.TARGET, this.getMetaName(), this.getMetaID(), exception, "target notify exception", new Object[0]);
        }
    }
    
    @Override
    public void gracefulStopApplication(final Event event) {
        Target.logger.info((Object)"Graceful stopping Application");
        this.notifyAppMgrForStopApplication(EntityType.TARGET, this.getMetaName(), this.getMetaID(), null, "target notify graceful stop", event);
    }
    
    @Override
    public void bytesWritten(final long bytes) {
        this.bytesWritten += bytes;
    }
    
    @Override
    public void latency(final long latencyOfSend) {
        this.latency += latencyOfSend;
        this.maxLatency = Math.max(this.maxLatency, latencyOfSend);
    }
    
    public void setPublisher(final Publisher publisher) {
        this.dataSource = publisher;
    }
    
    static {
        Target.logger = Logger.getLogger((Class)Target.class);
    }
}
