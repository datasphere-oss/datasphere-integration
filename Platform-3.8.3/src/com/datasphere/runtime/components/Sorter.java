package com.datasphere.runtime.components;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.channels.*;
import java.util.concurrent.atomic.*;

import com.datasphere.runtime.monitor.*;
import com.datasphere.classloading.*;
import com.datasphere.proc.events.commands.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;
import java.util.*;

public class Sorter extends FlowComponent implements PubSub, Restartable, Compound, Runnable
{
    private static final Comparator<Key> comparator;
    private static Logger logger;
    private final MetaInfo.Sorter sorterInfo;
    private final InOutChannel[] channels;
    private Subscriber errorSink;
    private final Channel errorOutput;
    private volatile boolean running;
    private final ConcurrentSkipListMap<Key, Object> index;
    private final AtomicLong nextGen;
    private Future<?> task;
    private final long interval;
    private RecordKey boundKey;
    private final Pair<String, String>[] streamInfos;
    
    public Sorter(final MetaInfo.Sorter sorterInfo, final BaseServer srv) throws Exception {
        super(srv, sorterInfo);
        this.running = false;
        this.index = new ConcurrentSkipListMap<Key, Object>(Sorter.comparator);
        this.nextGen = new AtomicLong(1L);
        this.sorterInfo = sorterInfo;
        this.interval = TimeUnit.MICROSECONDS.toNanos(sorterInfo.sortTimeInterval.value);
        this.errorOutput = srv.createSimpleChannel();
        this.channels = new InOutChannel[sorterInfo.inOutRules.size()];
        final Iterator<MetaInfo.Sorter.SorterRule> it = sorterInfo.inOutRules.iterator();
        this.streamInfos = (Pair<String, String>[])new Pair[sorterInfo.inOutRules.size()];
        int i = 0;
        while (it.hasNext()) {
            final MetaInfo.Sorter.SorterRule r = it.next();
            final InOutChannel[] channels = this.channels;
            final int n = i;
            final InOutChannel inOutChannel = new InOutChannel();
            channels[n] = inOutChannel;
            final InOutChannel c = inOutChannel;
            c.output = srv.createSimpleChannel();
            final MetaInfo.Stream streamInfo = srv.getStreamInfo(r.inStream);
            final String uniqueID = "_" + System.nanoTime();
            this.streamInfos[i] = Pair.make(streamInfo.nsName, streamInfo.name + uniqueID);
            c.keyFactory = KeyFactory.createKeyFactory(streamInfo.nsName, streamInfo.name + uniqueID, Collections.singletonList(r.inStreamField), r.inOutType, srv);
            ++i;
        }
    }
    
    @Override
    public Channel getChannel() {
        return this.channels[0].output;
    }
    
    @Override
    public void close() throws Exception {
        this.stop();
        for (final InOutChannel c : this.channels) {
            c.output.close();
        }
        for (final Pair<String, String> stream : this.streamInfos) {
            final WALoader wal = WALoader.get();
            wal.removeBundle(stream.first, BundleDefinition.Type.keyFactory, stream.second);
        }
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
    }
    
    @Override
    public synchronized void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        final boolean wasEmpty = this.index.isEmpty();
        final int channel_index = (int)linkID;
        final long curtime = System.nanoTime();
        final IBatch<DARecord> DARecordIBatch = (IBatch<DARecord>)event.batch();
        for (final DARecord ev : DARecordIBatch) {
            final Object o = ev.data;
            final long id = this.nextGen.getAndIncrement();
            final RecordKey key = this.channels[channel_index].keyFactory.makeKey(o);
            final Key k = new Key(key, channel_index, id);
            final Key higher = this.index.higherKey(k);
            k.timestamp = ((higher == null) ? curtime : higher.timestamp);
            this.index.put(k, o);
        }
        if (wasEmpty) {
            this.schedule(this.interval);
        }
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        final Iterator<MetaInfo.Sorter.SorterRule> it = this.sorterInfo.inOutRules.iterator();
        for (final InOutChannel c : this.channels) {
            final MetaInfo.Sorter.SorterRule r = it.next();
            c.dataSource = flow.getPublisher(r.inStream);
            c.dataSink = flow.getSubscriber(r.outStream);
        }
        this.errorSink = flow.getSubscriber(this.sorterInfo.errorStream);
    }
    
    @Override
    public void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        this.boundKey = null;
        int i = 0;
        final BaseServer s = this.srv();
        for (final InOutChannel c : this.channels) {
            s.subscribe(c.dataSource, new Link(this, i));
            s.subscribe(c.output, new Link(c.dataSink));
            ++i;
        }
        s.subscribe(this.errorOutput, new Link(this.errorSink));
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.running = false;
        int i = 0;
        final BaseServer s = this.srv();
        for (final InOutChannel c : this.channels) {
            s.unsubscribe(c.dataSource, new Link(this, i));
            s.unsubscribe(c.output, new Link(c.dataSink));
            ++i;
        }
        s.unsubscribe(this.errorOutput, new Link(this.errorSink));
        if (this.task != null) {
            this.task.cancel(true);
            this.task = null;
        }
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    private void schedule(final long delay) {
        final ScheduledExecutorService service = this.srv().getScheduler();
        this.task = service.schedule(this, delay, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void run() {
        try {
            while (this.running) {
                final long curtime = System.nanoTime();
                final Key k = this.index.firstKey();
                if (k.timestamp + this.interval + 50000L > curtime) {
                    final long leftToWait = k.timestamp + this.interval - curtime;
                    this.schedule(leftToWait);
                    return;
                }
                final Map.Entry<Key, Object> e = this.index.pollFirstEntry();
                final Key key = e.getKey();
                final Object val = e.getValue();
                if (this.boundKey == null || key.key.compareTo(this.boundKey) >= 0) {
                    this.boundKey = key.key;
                    this.channels[key.channel_index].output.publish(StreamEventFactory.createStreamEvent(val));
                }
                else {
                    this.errorOutput.publish(StreamEventFactory.createStreamEvent(val));
                }
            }
        }
        catch (NoSuchElementException ex) {}
        catch (RejectedExecutionException e3) {
            Sorter.logger.warn((Object)"sorter terminated due to shutdown");
        }
        catch (Throwable e2) {
            Sorter.logger.error((Object)e2);
        }
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        for (final InOutChannel inOutChannel : this.channels) {
            inOutChannel.output.publish(event);
        }
    }
    
    static {
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
        Sorter.logger = Logger.getLogger((Class)Sorter.class);
    }
    
    static class Key
    {
        final RecordKey key;
        final long id;
        long timestamp;
        final int channel_index;
        
        Key(final RecordKey key, final int channel_index, final long id) {
            this.key = key;
            this.channel_index = channel_index;
            this.id = id;
            this.timestamp = 0L;
        }
    }
    
    static class InOutChannel
    {
        Publisher dataSource;
        Subscriber dataSink;
        Channel output;
        KeyFactory keyFactory;
    }
}
