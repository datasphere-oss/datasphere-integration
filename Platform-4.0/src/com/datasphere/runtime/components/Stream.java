package com.datasphere.runtime.components;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.distribution.HQueue;
import com.datasphere.proc.events.ShowStreamEvent;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.proc.records.Record;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.StreamEventFactory;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.channels.DistributedChannel;
import com.datasphere.runtime.channels.KafkaChannel;
import com.datasphere.runtime.channels.ZMQChannel;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorCollector;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

@PropertyTemplate(name = "KafkaStream", type = AdapterType.stream, properties = { @PropertyTemplateProperty(name = "zk.address", type = String.class, required = true, defaultValue = "localhost:2181"), @PropertyTemplateProperty(name = "bootstrap.brokers", type = String.class, required = true, defaultValue = "localhost:9092"), @PropertyTemplateProperty(name = "partitions", type = Integer.class, required = false, defaultValue = "localhost:9092"), @PropertyTemplateProperty(name = "dataformat", type = String.class, required = false, defaultValue = "") })
public class Stream extends FlowComponent implements PubSub, Runnable
{
    private static Logger logger;
    private static Map<String, Integer> allTungstenShowStreams;
    private final MetaInfo.Stream streamInfo;
    private DistributedChannel output;
    private final Map<UUID, Flow> connectedFlows;
    private volatile long inputTotal;
    private final boolean hasDelayBuffer;
    private KeyFactory keyFactory;
    private ConcurrentSkipListMap<Key, Object> index;
    private AtomicLong nextGen;
    private long interval;
    private Future<?> task;
    private RecordKey boundKey;
    private final Map<String, ActiveShowStream> activeShows;
    private boolean isRecoveryEnabled;
    boolean lagReportEnabled;
    Long prevInputTotal;
    Long prevInputRate;
    Boolean prevFull;
    private static final Comparator<Key> comparator;
    
    public Stream(final MetaInfo.Stream streamInfo, final BaseServer srv, final Flow flow) throws Exception {
        super(srv, streamInfo);
        this.connectedFlows = new HashMap<UUID, Flow>();
        this.inputTotal = 0L;
        this.nextGen = new AtomicLong(1L);
        this.activeShows = new HashMap<String, ActiveShowStream>();
        this.isRecoveryEnabled = false;
        this.lagReportEnabled = Boolean.parseBoolean(System.getProperty("com.striim.lagReportEnabled", "false"));
        this.prevInputTotal = null;
        this.prevInputRate = null;
        this.prevFull = null;
        this.streamInfo = streamInfo;
        this.setFlow(flow);
        if (flow != null) {
            this.isRecoveryEnabled = flow.recoveryIsEnabled();
        }
        if (srv == null) {
            if (streamInfo.pset != null) {
                this.output = new KafkaChannel(this);
            }
            else {
                this.output = new ZMQChannel(this);
            }
        }
        else if (streamInfo.pset != null) {
            this.output = new KafkaChannel(this);
        }
        else {
            this.output = srv.getDistributedChannel(this);
        }
        this.hasDelayBuffer = (streamInfo.gracePeriodInterval != null);
        if (this.hasDelayBuffer) {
            this.interval = TimeUnit.MICROSECONDS.toNanos(streamInfo.gracePeriodInterval.value);
            this.keyFactory = KeyFactory.createKeyFactory(streamInfo, Collections.singletonList(streamInfo.gracePeriodField), streamInfo.dataType, srv);
            this.index = new ConcurrentSkipListMap<Key, Object>(Stream.comparator);
            this.nextGen = new AtomicLong(1L);
        }
    }
    
    private void addToAllTungstenShowStreams(final String queueName) {
        Integer count = Stream.allTungstenShowStreams.get(queueName);
        count = ((count == null) ? 1 : (count + 1));
        Stream.allTungstenShowStreams.put(queueName, count);
    }
    
    private void removeFromAllTungstenShowStreams(final String queueName) {
        Integer count = Stream.allTungstenShowStreams.get(queueName);
        count = ((count == null) ? 0 : (count - 1));
        if (count > 0) {
            Stream.allTungstenShowStreams.put(queueName, count);
        }
        else {
            Stream.allTungstenShowStreams.remove(queueName);
        }
    }
    
    public void initQueue(final MetaInfo.ShowStream showStream) {
        final String queueName = "consoleQueue" + showStream.session_id;
        synchronized (this.activeShows) {
            if (showStream.show) {
                ActiveShowStream ass = this.activeShows.get(queueName);
                if (ass == null) {
                    ass = new ActiveShowStream();
                    ass.consoleQueue = HQueue.getQueue(queueName);
                    ass.consoleQueue.isActive = true;
                    this.activeShows.put(queueName, ass);
                    if (showStream.isTungsten) {
                        this.addToAllTungstenShowStreams(queueName);
                    }
                }
                if (showStream.line_count > 0) {
                    ass.limit_queue = new AtomicInteger(showStream.line_count);
                }
            }
            else if (this.activeShows.containsKey(queueName)) {
                this.activeShows.remove(queueName);
                if (showStream.isTungsten) {
                    this.removeFromAllTungstenShowStreams(queueName);
                }
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        this.resetProcessThread();
        if (this.output != null) {
            this.output.close();
            this.output = null;
        }
        if (this.task != null) {
            this.task.cancel(true);
            this.task = null;
        }
        if (Stream.logger.isInfoEnabled()) {
            Stream.logger.info((Object)("Closing stream: " + this.getMetaFullName()));
        }
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (this.lagReportEnabled && this.shouldMarkerBePassedAlong(event)) {
            this.recordLagMarker(((TaskEvent)event).getLagMarker());
        }
        if (this.paused) {
            Stream.logger.warn((Object)("Stream " + this.getMetaName() + " dropping event received while quiesced: " + event));
            return;
        }
        try {
            if (this.isFlowInError()) {
                return;
            }
            this.setProcessThread();
            if (this.activeShows.size() > 0) {
                synchronized (this.activeShows) {
                    List<String> removeList = null;
                    for (final Map.Entry<String, ActiveShowStream> showEntry : this.activeShows.entrySet()) {
                        final ActiveShowStream show = showEntry.getValue();
                        if ((show.limit_queue == null || (show.limit_queue != null && show.limit_queue.get() != 0)) && show.consoleQueue.isActive) {
                            for (final DARecord E : event.batch()) {
                                show.consoleQueue.put(new ShowStreamEvent(this.streamInfo, E));
                            }
                            show.limit_queue.decrementAndGet();
                        }
                        else {
                            if (removeList == null) {
                                removeList = new ArrayList<String>();
                            }
                            removeList.add(showEntry.getKey());
                        }
                    }
                    if (removeList != null) {
                        for (final String queueName : removeList) {
                            this.activeShows.remove(queueName);
                            this.removeFromAllTungstenShowStreams(queueName);
                        }
                    }
                }
            }
            if (this.hasDelayBuffer) {
                this.addToDelayBuffer(event);
            }
            else if (this.output != null) {
                this.output.publish(event);
            }
            synchronized (this) {
                if (!event.getEventType().equals((Object)ITaskEvent.EVENTTYPE.CommandEvent)) {
                    ++this.inputTotal;
                }
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
        catch (Exception ex) {
            Stream.logger.error((Object)("exception receiving events by stream:" + this.streamInfo.name));
            this.notifyAppMgr(EntityType.STREAM, this.getMetaName(), this.getMetaID(), ex, "stream receive", event);
            throw new Exception(ex);
        }
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final Long it = this.inputTotal;
        if (this.output != null) {
            final boolean isFull = this.output.isFull();
            final long timeStamp = monEvs.getTimeStamp();
            if (!it.equals(this.prevInputTotal)) {
                monEvs.add(MonitorEvent.Type.INPUT, it);
                monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
            }
            if (this.prevTimeStamp != null && timeStamp != this.prevTimeStamp) {
                final Long ir = (long)Math.ceil(1000.0 * (it - this.prevInputTotal) / (timeStamp - this.prevTimeStamp));
                if (!ir.equals(this.prevInputRate)) {
                    monEvs.add(MonitorEvent.Type.RATE, ir);
                    monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
                    this.prevInputRate = ir;
                }
            }
            if (this.prevFull == null || this.prevFull != isFull) {
                monEvs.add(MonitorEvent.Type.STREAM_FULL, String.valueOf(isFull));
            }
            this.prevInputTotal = it;
            this.prevFull = isFull;
            if (this.output instanceof KafkaChannel) {
                final Map sMap = ((KafkaChannel)this.output).getStats();
                ((KafkaChannel)this.output).addSpecificMonitorEvents(monEvs);
            }
        }
    }
    
    @Override
    public String toString() {
        return this.getMetaUri() + " - " + this.getMetaID() + " RUNTIME";
    }
    
    public DistributedChannel getDistributedChannel() {
        return this.output;
    }
    
    public void connectedFlow(final Flow f) {
        this.connectedFlows.put(f.getMetaID(), f);
    }
    
    public void disconnectFlow(final Flow f) {
        this.connectedFlows.remove(f.getMetaID());
    }
    
    public boolean isConnected() {
        return !this.connectedFlows.isEmpty();
    }
    
    private void addToDelayBuffer(final ITaskEvent event) {
        final boolean wasEmpty = this.index.isEmpty();
        final long curtime = System.nanoTime();
        for (final DARecord ev : event.batch()) {
            final Object o = ev.data;
            final long id = this.nextGen.getAndIncrement();
            final RecordKey key = this.keyFactory.makeKey(o);
            final Key k = new Key(key, id);
            final Key higher = this.index.higherKey(k);
            k.timestamp = ((higher == null) ? curtime : higher.timestamp);
            this.index.put(k, o);
        }
        if (wasEmpty) {
            this.schedule(this.interval);
        }
    }
    
    private void schedule(final long delay) {
        final ScheduledExecutorService service = this.srv().getScheduler();
        this.task = service.schedule(this, delay, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void run() {
        try {
            long curtime;
            Key k;
            while (true) {
                curtime = System.nanoTime();
                k = this.index.firstKey();
                if (k.timestamp + this.interval + 50000L > curtime) {
                    break;
                }
                final Map.Entry<Key, Object> e = this.index.pollFirstEntry();
                final Key key = e.getKey();
                final Object val = e.getValue();
                if (this.boundKey != null && key.key.compareTo(this.boundKey) < 0) {
                    continue;
                }
                this.boundKey = key.key;
                this.output.publish(StreamEventFactory.createStreamEvent(val));
            }
            final long leftToWait = k.timestamp + this.interval - curtime;
            this.schedule(leftToWait);
        }
        catch (NoSuchElementException ex) {}
        catch (RejectedExecutionException e3) {
            Stream.logger.warn((Object)"stream delay buffer terminated due to shutdown");
        }
        catch (Throwable e2) {
            Stream.logger.error((Object)e2);
        }
    }
    
    @Override
    public Position getCheckpoint() {
        if (this.output == null) {
            return null;
        }
        final Position result = this.output.getCheckpoint();
        if (this.hasDelayBuffer) {}
        return result;
    }
    
    public MetaInfo.Stream getStreamMeta() {
        return this.streamInfo;
    }
    
    @Override
    public synchronized void stop() {
        if (this.output != null) {
            this.output.stop();
        }
        final long time = System.currentTimeMillis();
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.INPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
    }
    
    public void start() {
        if (this.output != null) {
            this.output.start();
        }
        this.paused = false;
    }
    
    public void start(final Map<UUID, Set<UUID>> servers) throws Exception {
        if (this.output != null) {
            this.output.start(servers);
        }
        final Flow flow = this.getFlow();
        final Flow topLevelFlow = flow.getTopLevelFlow();
        final MetaInfo.Flow flowMeta = topLevelFlow.getFlowMeta();
        final UUID id = this.getMetaID();
        final Set<UUID> publishers = flowMeta.getInputs(id);
        final UUID serverID = this.srv().getServerID();
        int clusterPublishers = 0;
        int nodePublishers = 0;
        for (final UUID key : servers.keySet()) {
            final Set<UUID> uuids = servers.get(key);
            for (final UUID uuid : uuids) {
                if (publishers.contains(uuid)) {
                    ++clusterPublishers;
                    if (!key.equals((Object)serverID)) {
                        continue;
                    }
                    ++nodePublishers;
                }
            }
        }
        this.inDegreeNode = clusterPublishers;
        if (this.inDegreeNode <= 0 && Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Undetermined in-degree for component " + this.getMetaName() + ": " + this.inDegreeNode));
        }
    }
    
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) {
        return this.output == null || this.output.verifyStart(serverToDeployedObjects);
    }
    
    public void cleanChannelSubscriber(final String uuid) {
        if (this.getMetaInfo().pset == null) {
            this.output.closeDistSub(uuid);
        }
    }
    
    @Override
    public MetaInfo.Stream getMetaInfo() {
        return this.streamInfo;
    }
    
    public void setPositionAndStartEmitting(final PartitionedSourcePosition position) throws Exception {
        this.output.startEmitting((SourcePosition)position);
    }
    
    public boolean isRecoveryEnabled() {
        return this.isRecoveryEnabled;
    }
    
    public void setRecoveryEnabled(final boolean isRecoveryEnabled) {
        if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
            Logger.getLogger("KafkaStreams").debug((Object)("Stream " + this.getMetaName() + " set recovery=" + isRecoveryEnabled));
        }
        this.isRecoveryEnabled = isRecoveryEnabled;
    }
    
    @Override
    public void injectCommandEvent(final CommandEvent commandEvent) throws Exception {
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Stream " + this.getMetaName() + " is injecting " + commandEvent.getClass() + "  "));
        }
        if (!this.paused) {
            this.injectedCommandEvents.add(commandEvent);
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Stream " + this.getMetaName() + " injected command event " + commandEvent));
        }
    }
    
    static {
        Stream.logger = Logger.getLogger((Class)Stream.class);
        Stream.allTungstenShowStreams = new ConcurrentHashMap<String, Integer>();
        comparator = new Comparator<Key>() {
            @Override
            public int compare(final Key k1, final Key k2) {
                final int cmp = k1.key.compareTo(k2.key);
                if (cmp != 0) {
                    return cmp;
                }
                if (k1.id < k2.id) {
                    return -1;
                }
                if (k1.id > k2.id) {
                    return 1;
                }
                return 0;
            }
        };
    }
    
    private class ActiveShowStream
    {
        public HQueue consoleQueue;
        public AtomicInteger limit_queue;
        
        private ActiveShowStream() {
            this.consoleQueue = null;
            this.limit_queue = null;
        }
    }
    
    private static class Key
    {
        final RecordKey key;
        final long id;
        long timestamp;
        
        Key(final RecordKey key, final long id) {
            this.key = key;
            this.id = id;
            this.timestamp = 0L;
        }
    }
}
