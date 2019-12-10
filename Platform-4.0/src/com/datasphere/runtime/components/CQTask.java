package com.datasphere.runtime.components;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.datasphere.classloading.HDLoader;
import com.datasphere.distribution.HQueue;
import com.datasphere.event.Event;
import com.datasphere.event.SimpleEvent;
import com.datasphere.exception.RuntimeInterruptedException;
import com.datasphere.exception.ServerException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.select.ParamDesc;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.IRange;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorCollector;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.window.Window;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;

public class CQTask extends FlowComponent implements PubSub, Compound
{
    private static Logger logger;
    private final MetaInfo.CQ cqinfo;
    private final CQExecutionPlan plan;
    private Map<RecordKey, AggMapEntry> aggTable;
    private Object[] params;
    private final Map<String, ParamDesc> paramsMap;
    private IRange[] state;
    private final int count;
    private final Class<?>[] subTaskFactories;
    private WindowIndex[] indexes;
    private Channel output;
    private volatile long inputTotal;
    private volatile long outputTotal;
    private volatile boolean running;
    private Subscriber dataSink;
    Publisher[] dataSources;
    private ITaskEvent curInput;
    private final TraslatedSchemaCache schemaCache;
    private final CQPatternMatcher matcher;
    private TraceOptions traceOptions;
    private HQueue outQueue;
    private PathManager waitPosition;
    Boolean lagReportEnabled;
    byte[] isDataReceived;
    private ThreadLocal lagRecordCache;
    private final Queue<LagMarker> lagMarkerQueue;
    private Long prevIt;
    private Long prevInputRate;
    private Long prevOt;
    private Long prevOutputRate;
    private int outStats;
    private int consoleInStatsSnapshot;
    private int consoleInStatsAdded;
    private int consoleInStatsRemoved;
    private int consoleOutStatsAdded;
    private int consoleOutStatsRemoved;
    Boolean allSourcesReady;
    Boolean atLeastOneDSisStreaming;
    
    public CQTask(final MetaInfo.CQ cqinfo, final BaseServer srv) throws Exception {
        super(srv, cqinfo);
        this.inputTotal = 0L;
        this.outputTotal = 0L;
        this.running = false;
        this.outQueue = null;
        this.waitPosition = null;
        this.lagReportEnabled = Boolean.parseBoolean(System.getProperty("com.dss.lagReportEnabled", "false"));
        this.lagRecordCache = new ThreadLocal();
        this.lagMarkerQueue = new ConcurrentLinkedQueue<LagMarker>();
        this.prevIt = null;
        this.prevInputRate = null;
        this.prevOt = null;
        this.prevOutputRate = null;
        this.outStats = 0;
        this.consoleInStatsSnapshot = 0;
        this.consoleInStatsAdded = 0;
        this.consoleInStatsRemoved = 0;
        this.consoleOutStatsAdded = 0;
        this.consoleOutStatsRemoved = 0;
        this.allSourcesReady = null;
        final ClassLoader cl = HDLoader.get();
        assert cl != null;
        this.cqinfo = cqinfo;
        this.plan = cqinfo.plan;
        this.output = srv.createSimpleChannel();
        this.count = this.plan.dataSources.size();
        this.subTaskFactories = (Class<?>[])new Class[this.count];
        this.params = new Object[this.plan.paramsDesc.size()];
        this.paramsMap = Factory.makeNameMap();
        this.traceOptions = cqinfo.plan.traceOptions;
        for (final ParamDesc d : this.plan.paramsDesc) {
            this.paramsMap.put(d.paramName, d);
        }
        final CQLoader classLoader = new CQLoader(cl);
        int i = 0;
        for (final byte[] subtask : this.plan.code) {
            final Class<?> subTaskClass = classLoader.loadByteCode(subtask);
            final Method m = subTaskClass.getMethod("initStatic", (Class<?>[])new Class[0]);
            m.invoke(null, new Object[0]);
            this.subTaskFactories[i++] = subTaskClass;
        }
        this.schemaCache = createSchemaCache(this.plan.dataSources, srv);
        if (this.plan.isMatch()) {
            (this.matcher = (CQPatternMatcher)this.subTaskFactories[0].newInstance()).init(this, srv.getScheduler());
        }
        else {
            this.matcher = null;
        }
    }
    
    public void setTraceOptions(final TraceOptions to, final HQueue clientQueue) {
        this.traceOptions = to;
        this.outQueue = clientQueue;
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSink = ((this.cqinfo.stream != null) ? flow.getSubscriber(this.cqinfo.stream) : null);
        this.dataSources = new Publisher[this.count];
        this.isDataReceived = new byte[this.count];
        int i = 0;
        for (final CQExecutionPlan.DataSource ds : this.plan.dataSources) {
            this.isDataReceived[i] = 0;
            this.dataSources[i++] = flow.getPublisher(ds.getDataSourceID());
        }
    }
    
    public Stream connectStream() throws Exception {
        this.dataSink = ((this.cqinfo.stream != null) ? this.srv().getStream(this.cqinfo.stream, null) : null);
        return (Stream)this.dataSink;
    }
    
    public void connectDataSources() {
        this.dataSources = new Publisher[this.count];
        this.isDataReceived = new byte[this.count];
        int i = 0;
        for (final CQExecutionPlan.DataSource ds : this.plan.dataSources) {
            this.isDataReceived[i] = 0;
            this.dataSources[i++] = (Publisher)this.srv().getOpenObject(ds.getDataSourceID());
        }
    }
    
    public synchronized void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        this.paused = false;
        try {
            this.aggTable = Factory.makeMap();
            this.state = new IRange[this.count];
            for (int i = 0; i < this.count; ++i) {
                this.state[i] = (IRange)Range.emptyRange();
            }
            this.indexes = new WindowIndex[this.plan.indexCount];
            for (int i = 0; i < this.plan.indexCount; ++i) {
                this.indexes[i] = new WindowIndex();
            }
            if (this.dataSink != null) {
                this.srv().subscribe(this, this.dataSink);
            }
            for (int i = 0; i < this.count; ++i) {
                this.srv().subscribe(this.dataSources[i], new Link(this, i));
            }
        }
        catch (Exception e) {
            this.running = false;
            throw e;
        }
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        try {
            if (this.matcher != null) {
                this.matcher.stop();
            }
            if (this.dataSink != null) {
                this.srv().unsubscribe(this, this.dataSink);
            }
            for (int i = 0; i < this.count; ++i) {
                this.srv().unsubscribe(this.dataSources[i], new Link(this, i));
            }
        }
        finally {
            this.resetProcessThread();
            this.running = false;
        }
        final long time = System.currentTimeMillis();
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.OUTPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.INPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
    }
    
    @Override
    public void close() throws Exception {
        if (CQTask.logger.isDebugEnabled()) {
            CQTask.logger.debug((Object)("Closing CQ " + this.getMetaName() + "..."));
        }
        if (CQTask.logger.isInfoEnabled()) {
            CQTask.logger.info((Object)("cq " + this.getMetaName() + " processed " + this.outputTotal));
        }
        this.stop();
        this.output.close();
    }
    
    @Override
    public synchronized void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (this.lagReportEnabled) {
            this.lagRecordCache.set(null);
            if (this.shouldMarkerBePassedAlong(event)) {
                this.lagRecordInfo(this.cqinfo, event);
                if (this.matcher != null) {
                    this.lagMarkerQueue.offer(((TaskEvent)event).getLagMarker().copy());
                }
                else {
                    this.lagRecordCache.set(((TaskEvent)event).getLagMarker().copy());
                }
            }
        }
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        if (this.paused) {
            CQTask.logger.warn((Object)("Stream " + this.getMetaName() + " dropping event received while paused: " + event));
            return;
        }
        this.curInput = event;
        if (!this.running || this.isFlowInError()) {
            return;
        }
        if (this.waitPosition != null) {
            final List<DARecord> exceedsEvents = new ArrayList<DARecord>();
            for (final DARecord e : event.batch()) {
                if (e.position == null || !this.waitPosition.blocks(e.position)) {
                    exceedsEvents.add(e);
                    if (e.position == null || this.waitPosition == null) {
                        continue;
                    }
                    final Position removeThesePaths = e.position.createAugmentedPosition(this.getMetaID(), (String)null);
                    this.waitPosition.removePaths(removeThesePaths.keySet());
                }
            }
            if (exceedsEvents.size() == 0) {
                return;
            }
            if (exceedsEvents.size() < event.batch().size()) {
                Batch.asBatch(exceedsEvents);
            }
        }
        if (this.waitPosition != null && this.waitPosition.isEmpty()) {
            this.waitPosition = null;
        }
        this.curInput = event;
        if (!this.running || this.isFlowInError()) {
            return;
        }
        this.setProcessThread();
        try {
            this.inputTotal += event.batch().size();
            final int streamID = (int)linkID;
            this.dumpInput(event, streamID);
            if ((event.getFlags() & 0x2) != 0x0) {
                this.aggTable = Factory.makeMap();
            }
            if (event.snapshotUpdate()) {
                final IRange r = event.snapshot();
                this.state[streamID] = r;
                return;
            }
            if (this.matcher != null) {
                this.matcher.addBatch(streamID, event);
            }
            else {
                this.isDataReceived[streamID] = 1;
                final Class<?> taskFactory = this.subTaskFactories[streamID];
                final CQSubTask task = (CQSubTask)taskFactory.newInstance();
                task.init(this);
                task.setEvent(event);
                synchronized (this) {
                    this.state[streamID] = this.state[streamID].update(event.snapshot());
                    task.updateState();
                }
                task.run();
                task.cleanState();
            }
        }
        catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            else {
                CQTask.logger.error((Object)("Problem running CQ " + this.cqinfo.name + " for event " + event + " : "), (Throwable)ex);
                this.notifyAppMgr(EntityType.CQ, this.cqinfo.name, this.getMetaID(), ex, "cq receive", event);
            }
            throw ex;
        }
    }
    
    @Override
    public void flush() throws Exception {
        if (this.matcher != null) {
            this.matcher.triggerTimersImmediately();
        }
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    IRange[] copyState() {
        return Arrays.copyOf(this.state, this.count);
    }
    
    void updateIndex(final int index, final RecordKey key, final DARecord value, final boolean doadd) {
        this.indexes[index].update(key, value, doadd);
    }
    
    public Object[] getAggVec(final RecordKey key, final boolean isAdd) {
        final Map<RecordKey, AggMapEntry> aggs = this.aggTable;
        AggMapEntry val = aggs.get(key);
        assert !(!isAdd);
        Object[] aggVec;
        if (val == null) {
            aggVec = this.createNewAggVec();
            val = new AggMapEntry(aggVec);
            aggs.put(key, val);
        }
        else {
            aggVec = val.aggregates;
        }
        if (isAdd) {
            final AggMapEntry aggMapEntry = val;
            ++aggMapEntry.refCount;
        }
        else {
            final AggMapEntry aggMapEntry2 = val;
            --aggMapEntry2.refCount;
            if (val.refCount == 0) {
                aggs.remove(key);
            }
        }
        return aggVec;
    }
    
    private Object[] createNewAggVec() {
        final List<Class<?>> aggObjFactories = this.plan.aggObjFactories;
        final int aggObjCount = aggObjFactories.size();
        final Object[] aggVec = new Object[aggObjCount];
        for (int i = 0; i < aggObjCount; ++i) {
            try {
                aggVec[i] = aggObjFactories.get(i).newInstance();
            }
            catch (InstantiationException | IllegalAccessException ex2) {
                CQTask.logger.error(ex2);
            }
        }
        return aggVec;
    }
    
    int getStreamCount() {
        return this.count;
    }
    
    int getDataSetCount() {
        return this.plan.dataSetCount;
    }
    
    Iterator<DARecord> createIndexIterator(final int index, final RecordKey key) {
        return this.indexes[index].createIterator(key);
    }
    
    void doOutput(final List<DARecord> xnew, final List<DARecord> xold) {
        this.dumpOutput(xnew, xold);
        if (xnew.isEmpty() && xold.isEmpty()) {
            return;
        }
        if (!this.canOutput()) {
            return;
        }
        ++this.outputTotal;
        try {
            final TaskEvent event = TaskEvent.createStreamEvent(xnew, xold, this.plan.isStateful());
            LagMarker lagMarker = null;
            if (this.matcher != null) {
                lagMarker = this.lagMarkerQueue.poll();
            }
            else {
                lagMarker = (LagMarker)this.lagRecordCache.get();
            }
            if (lagMarker != null) {
                this.recordLagMarker(lagMarker);
                event.setLagRecord(true);
                event.setLagMarker(lagMarker);
                this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)event);
            }
            this.output.publish((ITaskEvent)event);
        }
        catch (RuntimeInterruptedException e) {
            throw e;
        }
        catch (Exception e2) {
            if (e2 instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            else {
                CQTask.logger.error((Object)(this.getMetaName() + " unable to run the CQ Task!"), (Throwable)e2);
            }
        }
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    private boolean canOutput() {
        switch (this.plan.kindOfOutputStream) {
            case 1: {
                return !this.curInput.batch().isEmpty();
            }
            case 2: {
                return !this.curInput.removedBatch().isEmpty();
            }
            default: {
                return true;
            }
        }
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final Long it = this.inputTotal;
        final Long ot = this.outputTotal;
        final long timeStamp = monEvs.getTimeStamp();
        if (!it.equals(this.prevIt)) {
            monEvs.add(MonitorEvent.Type.INPUT, it);
        }
        if (this.prevTimeStamp != null) {
            final Long ir = (long)Math.ceil(1000.0 * (it - this.prevIt) / (timeStamp - this.prevTimeStamp));
            if (!ir.equals(this.prevInputRate)) {
                monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
                monEvs.add(MonitorEvent.Type.RATE, ir);
                this.prevInputRate = ir;
            }
        }
        if (!ot.equals(this.prevOt)) {
            monEvs.add(MonitorEvent.Type.OUTPUT, ot);
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
        }
        if (this.prevTimeStamp != null) {
            final Long or = (long)Math.ceil(1000.0 * (ot - this.prevOt) / (timeStamp - this.prevTimeStamp));
            if (!or.equals(this.prevOutputRate)) {
                monEvs.add(MonitorEvent.Type.OUTPUT_RATE, or);
                this.prevOutputRate = or;
            }
        }
        this.prevIt = it;
        this.prevOt = ot;
    }
    
    @Override
    public String toString() {
        return this.getMetaUri() + " - " + this.getMetaID() + " RUNTIME";
    }
    
    TranslatedSchema getTranslatedSchema(final UUID dynamicEventTypeID, final int dataSetIndex) throws MetaDataRepositoryException {
        final SchemaTraslator[] translators = this.schemaCache.translators;
        final Map<UUID, TranslatedSchema> cache = this.schemaCache.schemaCache;
        if (translators[dataSetIndex].current.typeID.equals((Object)dynamicEventTypeID)) {
            return translators[dataSetIndex].current;
        }
        TranslatedSchema t = cache.get(dynamicEventTypeID);
        if (t == null) {
            t = createTranslatedSchema(dynamicEventTypeID, translators[dataSetIndex], this.srv());
            cache.put(dynamicEventTypeID, t);
        }
        return translators[dataSetIndex].current = t;
    }
    
    private static TraslatedSchemaCache createSchemaCache(final List<CQExecutionPlan.DataSource> dataSources, final BaseServer srv) throws ServerException, MetaDataRepositoryException {
        int i = 0;
        for (final CQExecutionPlan.DataSource ds : dataSources) {
            if (ds.typeID != null) {
                ++i;
            }
        }
        if (i == 0) {
            return null;
        }
        final TraslatedSchemaCache cache = new TraslatedSchemaCache(dataSources.size());
        i = 0;
        for (final CQExecutionPlan.DataSource ds2 : dataSources) {
            final UUID typeID = ds2.typeID;
            if (typeID != null) {
                final SchemaTraslator[] translators = cache.translators;
                final int n = i;
                final SchemaTraslator schemaTraslator = new SchemaTraslator();
                translators[n] = schemaTraslator;
                final SchemaTraslator st = schemaTraslator;
                st.originalTypeID = typeID;
                st.originalType = getTypeInfo(typeID, srv);
                st.current = createTranslatedSchema(typeID, st, srv);
            }
            ++i;
        }
        return cache;
    }
    
    private static TranslatedSchema createTranslatedSchema(final UUID toTypeID, final SchemaTraslator st, final BaseServer srv) throws MetaDataRepositoryException {
        boolean instanceOfExpectedType = false;
        final Map<String, Pair<String, Integer>> originalType = st.originalType;
        MetaInfo.Type toType;
        try {
            toType = srv.getTypeInfo(toTypeID);
            if (toTypeID.equals((Object)st.originalTypeID)) {
                instanceOfExpectedType = true;
            }
            else {
                for (MetaInfo.Type t = toType; t.extendsType != null; t = srv.getTypeInfo(t.extendsType)) {
                    if (t.extendsType.equals((Object)st.originalTypeID)) {
                        instanceOfExpectedType = true;
                        break;
                    }
                }
            }
        }
        catch (ServerException e) {
            throw new RuntimeException("cannot find dynamic type info", e);
        }
        final TranslatedSchema res = new TranslatedSchema();
        res.typeID = toTypeID;
        res.instanceOfExpectedType = instanceOfExpectedType;
        res.realFieldIndex = new int[toType.fields.size()];
        int i = 0;
        for (final Map.Entry<String, String> fld : toType.fields.entrySet()) {
            final String fieldName = fld.getKey();
            final String fieldType = fld.getValue();
            final Pair<String, Integer> typeAndIndex = originalType.get(fieldName);
            int index = -1;
            if (typeAndIndex != null && typeAndIndex.first.equals(fieldType)) {
                index = typeAndIndex.second;
            }
            res.realFieldIndex[i] = index;
            ++i;
        }
        return res;
    }
    
    private static Map<String, Pair<String, Integer>> getTypeInfo(final UUID typeID, final BaseServer srv) throws ServerException, MetaDataRepositoryException {
        final Map<String, Pair<String, Integer>> fields = new TreeMap<String, Pair<String, Integer>>(String.CASE_INSENSITIVE_ORDER);
        final MetaInfo.Type type = srv.getTypeInfo(typeID);
        int i = 0;
        for (final Map.Entry<String, String> fld : type.fields.entrySet()) {
            final String fieldName = fld.getKey();
            final String fieldType = fld.getValue();
            fields.put(fieldName, Pair.make(fieldType, i));
            ++i;
        }
        return fields;
    }
    
    private void dumpOutput(final List<DARecord> xnew, final List<DARecord> xold) {
        if ((this.plan.traceOptions.traceFlags & 0x2) > 0) {
            final boolean multiline = (this.plan.traceOptions.traceFlags & 0x20) != 0x0;
            final PrintStream out = TraceOptions.getTraceStream(this.plan.traceOptions);
            List<ObjectNode> lnodes = (List<ObjectNode>)this.getJsonFromOutputEvents(xnew, "added", Integer.MAX_VALUE);
            for (final ObjectNode lnode : lnodes) {
                if (out != null) {
                    out.println(lnode);
                }
            }
            if (xold != null && xold.size() > 0) {
                lnodes = (List<ObjectNode>)this.getJsonFromOutputEvents(xold, "removed", Integer.MAX_VALUE);
                for (final ObjectNode lnode : lnodes) {
                    if (out != null) {
                        out.println(lnode);
                    }
                }
            }
        }
        boolean dumpConsole = false;
        if ((this.traceOptions.traceFlags & 0x2) > 0) {
            final boolean multiline2 = (this.traceOptions.traceFlags & 0x20) != 0x0;
            if (this.consoleOutStatsAdded >= this.traceOptions.limit || this.consoleOutStatsRemoved >= this.traceOptions.limit) {
                final TraceOptions traceOptions = this.traceOptions;
                traceOptions.traceFlags &= 0xFFFFFFFD;
                final boolean b = false;
                this.consoleOutStatsRemoved = (b ? 1 : 0);
                this.consoleOutStatsAdded = (b ? 1 : 0);
                dumpConsole = false;
            }
            else {
                dumpConsole = true;
            }
        }
        if (dumpConsole) {
            if (this.consoleOutStatsAdded < this.traceOptions.limit) {
                final List<ObjectNode> lnodes2 = (List<ObjectNode>)this.getJsonFromOutputEvents(xnew, "added", this.traceOptions.limit - this.consoleOutStatsAdded);
                if (lnodes2 != null) {
                    for (final ObjectNode lnode2 : lnodes2) {
                        if (this.outQueue != null) {
                            this.outQueue.put(lnode2);
                        }
                    }
                    this.consoleOutStatsAdded += lnodes2.size();
                }
            }
            if (xold != null && xold.size() > 0 && this.consoleOutStatsRemoved < this.traceOptions.limit) {
                final List<ObjectNode> lnodes2 = (List<ObjectNode>)this.getJsonFromOutputEvents(xnew, "removed", this.traceOptions.limit - this.consoleOutStatsRemoved);
                if (lnodes2 != null) {
                    for (final ObjectNode lnode2 : lnodes2) {
                        if (this.outQueue != null) {
                            this.outQueue.put(lnode2);
                        }
                    }
                    this.consoleOutStatsRemoved += lnodes2.size();
                }
            }
        }
    }
    
    private void dumpInput(final ITaskEvent e, final int inputID) {
        if ((this.plan.traceOptions.traceFlags & 0x1) > 0) {
            final boolean multiline = (this.traceOptions.traceFlags & 0x20) != 0x0;
            final String inputName = this.plan.dataSources.get(inputID).name;
            final PrintStream out = TraceOptions.getTraceStream(this.plan.traceOptions);
            if (e.snapshotUpdate()) {
                final List<ObjectNode> lnodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.snapshot().all(), "new-snapshot", inputName, Integer.MAX_VALUE);
                if (lnodes != null) {
                    for (final ObjectNode lnode : lnodes) {
                        if (out != null) {
                            out.println(lnode);
                        }
                    }
                }
            }
            else {
                List<ObjectNode> lnodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.batch(), "added", inputName, Integer.MAX_VALUE);
                if (lnodes != null) {
                    for (final ObjectNode lnode : lnodes) {
                        if (out != null) {
                            out.println(lnode);
                        }
                    }
                }
                lnodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.removedBatch(), "removed", inputName, Integer.MAX_VALUE);
                if (lnodes != null) {
                    for (final ObjectNode lnode : lnodes) {
                        if (out != null) {
                            out.println(lnode);
                        }
                    }
                }
            }
        }
        boolean dumpConsole = false;
        if ((this.traceOptions.traceFlags & 0x1) > 0) {
            final boolean multiline2 = (this.traceOptions.traceFlags & 0x20) != 0x0;
            if (this.consoleInStatsSnapshot >= this.traceOptions.limit || this.consoleInStatsAdded >= this.traceOptions.limit || this.consoleInStatsRemoved >= this.traceOptions.limit) {
                dumpConsole = false;
                final boolean consoleInStatsSnapshot = false;
                this.consoleInStatsRemoved = (consoleInStatsSnapshot ? 1 : 0);
                this.consoleInStatsAdded = (consoleInStatsSnapshot ? 1 : 0);
                this.consoleInStatsSnapshot = (consoleInStatsSnapshot ? 1 : 0);
                final TraceOptions traceOptions = this.traceOptions;
                traceOptions.traceFlags &= 0xFFFFFFFE;
            }
            else {
                dumpConsole = true;
            }
        }
        if (dumpConsole) {
            final String inputName = this.plan.dataSources.get(inputID).name;
            if (e.snapshotUpdate()) {
                if (this.consoleInStatsSnapshot < this.traceOptions.limit) {
                    final List<ObjectNode> lNodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.snapshot().all(), "new-snapshot", inputName, this.traceOptions.limit - this.consoleInStatsSnapshot);
                    for (final ObjectNode lnode2 : lNodes) {
                        if (this.outQueue != null) {
                            this.outQueue.put(lnode2);
                        }
                    }
                    this.consoleInStatsSnapshot += lNodes.size();
                }
            }
            else {
                if (this.consoleInStatsAdded < this.traceOptions.limit) {
                    final List<ObjectNode> lNodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.batch(), "added", inputName, this.traceOptions.limit - this.consoleInStatsAdded);
                    for (final ObjectNode lnode2 : lNodes) {
                        if (this.outQueue != null) {
                            this.outQueue.put(lnode2);
                        }
                    }
                    this.consoleInStatsAdded += lNodes.size();
                }
                if (this.consoleInStatsRemoved < this.traceOptions.limit) {
                    final List<ObjectNode> lNodes = (List<ObjectNode>)this.getJsonFromInputEvents(e.removedBatch(), "removed", inputName, this.traceOptions.limit - this.consoleInStatsRemoved);
                    if (lNodes != null) {
                        for (final ObjectNode lnode2 : lNodes) {
                            if (this.outQueue != null) {
                                this.outQueue.put(lnode2);
                            }
                        }
                        this.consoleInStatsRemoved += lNodes.size();
                    }
                }
            }
        }
    }
    
    private List getJsonFromInputEvents(final IBatch l, final String what, final String datasrc, final int numEvents) {
        ArrayList<ObjectNode> al = null;
        int eventsDumped = 0;
        if (!l.isEmpty()) {
            al = new ArrayList<ObjectNode>(1);
            for (final Object x : l) {
                final ObjectNode stmt = JsonNodeFactory.instance.objectNode();
                stmt.put("datasource", datasrc);
                stmt.put("IO", "I");
                stmt.put("type", what);
                stmt.put("serverid", BaseServer.getServerName());
                stmt.put("queryname", this.getMetaFullName());
                final DARecord o = (DARecord)x;
                if (o.data instanceof Event) {
                    final Event e = (Event)o.data;
                    stmt.put("data", Arrays.deepToString(e.getPayload()));
                }
                else {
                    stmt.put("data", o.data.toString());
                }
                al.add(stmt);
                if (++eventsDumped == numEvents) {
                    break;
                }
            }
        }
        return al;
    }
    
    private List getJsonFromOutputEvents(final List<DARecord> l, final String what, final int numEvents) {
        ArrayList<ObjectNode> al = null;
        int eventsDumped = 0;
        if (!l.isEmpty()) {
            al = new ArrayList<ObjectNode>(1);
            for (final DARecord o : l) {
                final ObjectNode stmt = JsonNodeFactory.instance.objectNode();
                stmt.put("datasource", this.getMetaFullName());
                stmt.put("IO", "O");
                stmt.put("type", what);
                stmt.put("serverid", BaseServer.getServerName());
                stmt.put("queryname", this.getMetaFullName());
                final StringBuilder strbld = new StringBuilder();
                final SimpleEvent e = (SimpleEvent)o.data;
                strbld.append(Arrays.deepToString(e.getPayload()));
                strbld.append(";");
                if (e.linkedSourceEvents != null) {
                    strbld.append("H(");
                    for (final Object[] hobjs : e.linkedSourceEvents) {
                        strbld.append(hobjs.length).append("@");
                        for (final Object hobj : hobjs) {
                            strbld.append("{");
                            if (hobj != null) {
                                final SimpleEvent ee = (SimpleEvent)hobj;
                                strbld.append(Arrays.deepToString(ee.getPayload()));
                            }
                            else {
                                strbld.append("null");
                            }
                            strbld.append("}");
                        }
                    }
                    strbld.append(");");
                }
                stmt.put("data", strbld.toString());
                al.add(stmt);
                if (++eventsDumped == numEvents) {
                    break;
                }
            }
        }
        return al;
    }
    
    public Iterator<SimpleEvent> makeSnapshotIterator(final int ds) {
        return new Iterator<SimpleEvent>() {
            final Iterator<DARecord> it = CQTask.this.state[ds].all().iterator();
            
            @Override
            public boolean hasNext() {
                return this.it.hasNext();
            }
            
            @Override
            public SimpleEvent next() {
                return (SimpleEvent)this.it.next().data;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    String getSourceName(final int ds) {
        return this.cqinfo.plan.dataSources.get(ds).name;
    }
    
    boolean isTracingExecution() {
        return (this.traceOptions.traceFlags & 0x10) > 0;
    }
    
    PrintStream getTracingStream() {
        return TraceOptions.getTraceStream(this.traceOptions);
    }
    
    AggAlgo howToAggregate() {
        return this.plan.howToAggregate;
    }
    
    Object getParam(final int index) {
        return this.params[index];
    }
    
    void bindParam(final ParamDesc d, final Object val) {
        this.params[d.index] = val;
    }
    
    public int getParamIndex(final String paramName) {
        final ParamDesc d = this.paramsMap.get(paramName);
        if (d == null) {
            throw new RuntimeException("Invalid parameter's name <" + paramName + ">");
        }
        return d.index;
    }
    
    public void bindParameter(final String paramName, final Object val) {
        final ParamDesc d = this.paramsMap.get(paramName);
        if (d == null) {
            throw new RuntimeException("Invalid parameter's name <" + paramName + ">");
        }
        this.bindParam(d, val);
    }
    
    public void bindParameter(final int index, final Object val) {
        try {
            final ParamDesc d = this.plan.paramsDesc.get(index);
            this.bindParam(d, val);
        }
        catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("Parameter's index <" + index + "> is out of bounds");
        }
    }
    
    public boolean bindParameters(final List<Property> params) {
        if (params == null) {
            return this.paramsMap.isEmpty();
        }
        final BitSet pbmap = new BitSet(this.paramsMap.size());
        for (final Property p : params) {
            final ParamDesc d = this.paramsMap.get(p.name);
            if (d != null) {
                pbmap.set(d.index);
                this.bindParam(d, p.value);
            }
        }
        return pbmap.cardinality() == this.paramsMap.size();
    }
    
    public boolean allStreamingSourcesReady() {
        if (this.allSourcesReady != null) {
            return this.allSourcesReady;
        }
        for (final byte anIsDataReceived : this.isDataReceived) {
            if (anIsDataReceived == 0) {
                return false;
            }
        }
        this.allSourcesReady = true;
        return true;
    }
    
    boolean atLeastOneDSisStreaming() {
        if (this.atLeastOneDSisStreaming != null) {
            return this.atLeastOneDSisStreaming;
        }
        this.atLeastOneDSisStreaming = false;
        for (final Publisher pub : this.dataSources) {
            if (pub.getClass().getCanonicalName().equalsIgnoreCase(Stream.class.getCanonicalName()) || pub.getClass().getCanonicalName().equalsIgnoreCase(Window.class.getCanonicalName())) {
                this.atLeastOneDSisStreaming = true;
            }
        }
        return this.atLeastOneDSisStreaming;
    }
    
    public void setWaitPosition(final Position waitPosition) {
        this.waitPosition = new PathManager(waitPosition);
        if (CQTask.logger.isDebugEnabled()) {
            CQTask.logger.debug((Object)("Setting restart position for CQ " + this.getMetaName() + " to:"));
            Utility.prettyPrint(this.waitPosition);
        }
    }
    
    @Override
    public synchronized Position getCheckpoint() {
        final PathManager result = new PathManager();
        if (this.matcher != null) {
            result.mergeWiderPosition(this.matcher.getCheckpoint().values());
        }
        return result.toPosition();
    }
    
    static {
        CQTask.logger = Logger.getLogger((Class)CQTask.class);
    }
    
    private static class CQLoader extends ClassLoader
    {
        public CQLoader(final ClassLoader parent) {
            super(parent);
        }
        
        public Class<?> loadByteCode(final byte[] bytecode) {
            final String className = CompilerUtils.getClassName(bytecode);
            return this.defineClass(className, bytecode, 0, bytecode.length);
        }
    }
    
    private static class AggMapEntry
    {
        int refCount;
        final Object[] aggregates;
        
        AggMapEntry(final Object[] aggregates) {
            this.aggregates = aggregates.clone();
            this.refCount = 0;
        }
    }
    
    private static class SchemaTraslator
    {
        TranslatedSchema current;
        Map<String, Pair<String, Integer>> originalType;
        UUID originalTypeID;
    }
    
    private static class TraslatedSchemaCache
    {
        static final int cacheSize = 2000;
        final SchemaTraslator[] translators;
        final Map<UUID, TranslatedSchema> schemaCache;
        
        TraslatedSchemaCache(final int count) {
            this.translators = new SchemaTraslator[count];
            this.schemaCache = new LinkedHashMap<UUID, TranslatedSchema>(2000, 0.75f, true) {
                private static final long serialVersionUID = 1L;
                
                @Override
                protected boolean removeEldestEntry(final Map.Entry<UUID, TranslatedSchema> eldest) {
                    return this.size() > 2000;
                }
            };
        }
    }
}
