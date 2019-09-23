package com.datasphere.runtime.components;

import java.lang.reflect.Field;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.event.SimpleEvent;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.persistence.HStore;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.compiler.select.AST2JSON;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.hd.HD;
import com.datasphere.hd.HDKey;
import com.datasphere.hd.HDQueryResult;
import com.datasphere.hdstore.HD;
import com.datasphere.hdstore.HDQuery;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.HDStores;
import com.fasterxml.jackson.databind.JsonNode;

public class HStoreView extends IWindow implements Runnable
{
    private static Logger logger;
    private final HStore owner;
    private final MetaInfo.WAStoreView viewMetaInfo;
    private final Channel output;
    private final long timerLimit;
    private volatile boolean running;
    private Publisher dataSource;
    private long startTime;
    private long endTime;
    private ScheduledFuture<?> jumpingTask;
    private HDStoreManager cachedManager;
    private long inputTotal;
    private Long prevInputTotal;
    private Long prevInputRate;
    
    public HStoreView(final MetaInfo.WAStoreView viewMetaInfo, final HStore owner, final BaseServer srv) {
        super(srv, viewMetaInfo);
        this.running = false;
        this.inputTotal = 0L;
        this.prevInputTotal = null;
        this.prevInputRate = null;
        this.owner = owner;
        this.viewMetaInfo = viewMetaInfo;
        final IntervalPolicy viewIntervalPolicy = viewMetaInfo.viewSize;
        if (viewIntervalPolicy != null && viewIntervalPolicy.isTimeBased()) {
            this.timerLimit = viewIntervalPolicy.getTimePolicy().getTimeInterval();
        }
        else {
            this.timerLimit = 0L;
        }
        (this.output = srv.createChannel(this)).addCallback(this);
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
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSource = flow.getHDStore(this.viewMetaInfo.wastoreID);
    }
    
    @Override
    public String toString() {
        return this.getMetaUri() + " - " + this.getMetaID() + " RUNTIME";
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        this.output.publish(event);
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    @Override
    public void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        if (this.viewMetaInfo.subscribeToUpdates) {
            this.srv().subscribe(this.dataSource, this);
        }
        if (this.timerLimit > 0L && this.viewMetaInfo.isJumping) {
            this.startTime = System.currentTimeMillis();
            this.jumpingTask = this.srv().getScheduler().scheduleAtFixedRate(this, this.timerLimit, this.timerLimit, TimeUnit.MICROSECONDS);
        }
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.resetProcessThread();
        this.running = false;
        if (this.viewMetaInfo.subscribeToUpdates) {
            this.srv().unsubscribe(this.dataSource, this);
        }
        if (this.jumpingTask != null) {
            this.jumpingTask.cancel(true);
            this.jumpingTask = null;
        }
    }
    
    @Override
    public void notifyMe(final Link link) {
        try {
            Range r;
            if (this.timerLimit == 0L) {
                r = this.queryAllStore();
            }
            else if (!this.viewMetaInfo.isJumping) {
                final long end = System.currentTimeMillis();
                final long begin = end - TimeUnit.MICROSECONDS.toMillis(this.timerLimit);
                r = this.queryRange(begin, end);
            }
            else {
                r = Range.emptyRange();
            }
            final TaskEvent snapshotEvent = TaskEvent.createWindowStateEvent(r);
            link.subscriber.receive(link.linkID, (ITaskEvent)snapshotEvent);
        }
        catch (Exception e) {
            HStoreView.logger.error((Object)e);
            if (this.getFlow() != null && this.getFlow().getMetaInfo() != null && this.getFlow().getMetaInfo().getMetaInfoStatus().isAdhoc()) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }
    
    @Override
    public void run() {
        this.endTime = System.currentTimeMillis();
        try {
            final Range r = this.queryRange(this.startTime, this.endTime);
            final TaskEvent snapshotEvent = TaskEvent.createWAStoreQueryEvent(r);
            this.output.publish((ITaskEvent)snapshotEvent);
        }
        catch (Exception e) {
            HStoreView.logger.warn((Object)e.getMessage());
        }
        this.startTime = this.endTime + 1L;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final Long it = this.inputTotal;
        final long timeStamp = monEvs.getTimeStamp();
        if (!it.equals(this.prevInputTotal)) {
            monEvs.add(MonitorEvent.Type.INPUT, it);
        }
        if (this.prevTimeStamp != null) {
            final Long ir = 1000L * (it - this.prevInputTotal) / (timeStamp - this.prevTimeStamp);
            if (!ir.equals(this.prevInputRate)) {
                monEvs.add(MonitorEvent.Type.RATE, ir);
                monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
                this.prevInputRate = ir;
            }
        }
        this.prevInputTotal = it;
    }
    
    private Range executeQueryOldStyle(final long startTime, final long endTime) {
        return new Range() {
            private static final long serialVersionUID = -4805866185978952939L;
            private transient List<DARecord> values;
            
            public Batch all() {
                if (this.values == null) {
                    this.values = HStoreView.this.fetchHDs(startTime, endTime);
                }
                return Batch.asBatch(this.values);
            }
            
            @Override
            public String toString() {
                return "(wastore view range for time : " + new Date(startTime) + "-" + new Date(endTime) + ")";
            }
        };
    }
    
    public synchronized List<DARecord> fetchHDs(final long startTime, final long endTime) {
        final Collection<HD> res = this.owner.getHDs(startTime, endTime);
        return new AbstractList<DARecord>() {
            @Override
            public Iterator<DARecord> iterator() {
                return new Iterator<DARecord>() {
                    Iterator<HD> it = res.iterator();
                    
                    @Override
                    public boolean hasNext() {
                        return this.it.hasNext();
                    }
                    
                    @Override
                    public DARecord next() {
                        return new DARecord((Object)this.it.next());
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            @Override
            public DARecord get(final int index) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public int size() {
                return res.size();
            }
        };
    }
    
    private Class<?> getContextClass() {
        try {
            return ClassLoader.getSystemClassLoader().loadClass(this.owner.contextBeanDef.className);
        }
        catch (ClassNotFoundException e) {
            HStoreView.logger.warn((Object)e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    private SimpleEvent createContextEvent(final Class<?> clazz) {
        try {
            return (SimpleEvent)clazz.newInstance();
        }
        catch (InstantiationException | IllegalAccessException ex2) {
            HStoreView.logger.warn(ex2.getMessage());
            throw new RuntimeException(ex2);
        }
    }
    
    private void setField(final SimpleEvent ev, final Field fld, final String fieldName, final Map<String, Object> vals) {
        if (vals.containsKey(fieldName)) {
            try {
                final Object fieldVal = vals.get(fieldName);
                fld.set(ev, fieldVal);
            }
            catch (IllegalArgumentException | IllegalAccessException ex2) {
                HStoreView.logger.warn(ex2.getMessage());
                throw new RuntimeException(ex2);
            }
        }
    }
    
    private void setContextEventFieldsFromMap(final SimpleEvent ev, final Field[] fields, final Map<String, Object> vals) {
        if (ev.setFromContextMap((Map)vals)) {
            return;
        }
        ev.timeStamp = (long)vals.get("timestamp");
        for (final Field fld : fields) {
            final String fieldName = "context-" + fld.getName();
            this.setField(ev, fld, fieldName, vals);
        }
    }
    
    private synchronized List<DARecord> loadValues(final long startTime, final long endTime) {
        final Map<String, Object> settingsForFilter = new HashMap<String, Object>();
        settingsForFilter.put("singleHDs", "True");
        settingsForFilter.put("startTime", startTime);
        settingsForFilter.put("endTime", endTime);
        final Map<HDKey, Map<String, Object>> allHDs = this.owner.getHDs(null, null, settingsForFilter);
        final Class<?> clazz = this.getContextClass();
        final List<DARecord> resultSet = new ArrayList<DARecord>();
        if (allHDs != null) {
            for (final Map<String, Object> hd : allHDs.values()) {
                final SimpleEvent ev = this.createContextEvent(clazz);
                this.setContextEventFieldsFromMap(ev, clazz.getFields(), hd);
                resultSet.add(new DARecord((Object)ev));
            }
        }
        return resultSet;
    }
    
    private Range queryRange(final long startTime, final long endTime) throws MetaDataRepositoryException {
        if (this.viewMetaInfo.hasQuery()) {
            final JsonNode jsonQuery = this.getQuery();
            AST2JSON.addTimeBounds(jsonQuery, startTime, endTime);
            return this.executeQuery(jsonQuery);
        }
        return this.executeQueryOldStyle(startTime, endTime);
    }
    
    private Range queryAllStore() throws MetaDataRepositoryException {
        if (this.viewMetaInfo.hasQuery()) {
            return this.executeQuery(this.getQuery());
        }
        return this.executeQueryOldStyle(0L, System.currentTimeMillis());
    }
    
    private DARecord convert(final HD a) {
        final JsonNodeEvent e = new JsonNodeEvent();
        e.setData((JsonNode)a);
        return new DARecord((Object)e);
    }
    
    private Range makeSnapshot(final HDQueryResult rs) {
        return new Range() {
            private static final long serialVersionUID = 8193566130544575344L;
            
            public Batch all() {
                final Batch<DARecord> batch = new Batch<DARecord>() {
                    private static final long serialVersionUID = -6673129930236624672L;
                    
                    @Override
                    public Iterator<DARecord> iterator() {
                        final Iterator<HD> it = rs.iterator();
                        return new Iterator<DARecord>() {
                            @Override
                            public boolean hasNext() {
                                return it.hasNext();
                            }
                            
                            @Override
                            public DARecord next() {
                                return HStoreView.this.convert(it.next());
                            }
                            
                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                    
                    @Override
                    public int size() {
                        return (int)rs.size();
                    }
                    
                    @Override
                    public boolean isEmpty() {
                        return !this.iterator().hasNext();
                    }
                };
                return batch;
            }
            
            @Override
            public IBatch lookup(final int indexID, final RecordKey key) {
                return (IBatch)Batch.emptyBatch();
            }
            
            @Override
            public String toString() {
                return "(wastore view iterator range :" + rs + ")";
            }
        };
    }
    
    private JsonNode getQuery() {
        final JsonNode jsonQuery = AST2JSON.deserialize(this.viewMetaInfo.query);
        return jsonQuery;
    }
    
    private Range executeQuery(final JsonNode jsonQuery) throws MetaDataRepositoryException {
        if (this.cachedManager == null) {
            final MetaInfo.HDStore ws = (MetaInfo.HDStore)this.srv().getObject(this.viewMetaInfo.wastoreID);
            this.cachedManager = HDStores.getInstance(ws.properties);
        }
        final HDQuery q = this.cachedManager.prepareQuery(jsonQuery);
        return this.makeSnapshot(q.execute());
    }
    
    static {
        HStoreView.logger = Logger.getLogger((Class)HStoreView.class);
    }
}
