package com.datasphere.runtime.components;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;

public abstract class CQPatternMatcher
{
    private static Logger logger;
    private CQTask cqtask;
    private ScheduledThreadPoolExecutor scheduler;
    private AtomicInteger nextCtxId;
    private Map<RecordKey, MatcherPartition> partitions;
    
    public CQPatternMatcher() {
        this.nextCtxId = new AtomicInteger();
    }
    
    public void init(final CQTask cqTask, final ScheduledThreadPoolExecutor scheduler) {
        this.cqtask = cqTask;
        this.scheduler = scheduler;
        this.partitions = new HashMap<RecordKey, MatcherPartition>();
        boolean hasPartitions = false;
        try {
            if (!this.getClass().getMethod("makePartitionKey", Object.class).getDeclaringClass().equals(CQPatternMatcher.class)) {
                hasPartitions = true;
            }
        }
        catch (NoSuchMethodException ex) {}
        catch (SecurityException ex2) {}
        if (!hasPartitions) {
            final MatcherPartition mp = new MatcherPartition(null);
            this.partitions.put(null, mp);
        }
    }
    
    public void stop() {
        for (final MatcherPartition p : this.partitions.values()) {
            p.stop();
        }
        this.partitions.clear();
    }
    
    public void addBatch(final int streamID, final ITaskEvent event) {
        final IBatch<DARecord> DARecordIBatch = (IBatch<DARecord>)event.batch();
        for (final DARecord e : DARecordIBatch) {
            final RecordKey key = this.makePartitionKey(e.data);
            MatcherPartition p = this.partitions.get(key);
            if (p == null) {
                p = new MatcherPartition(key);
                this.partitions.put(key, p);
            }
            p.addEvent(streamID, e.data, e.position);
        }
    }
    
    ScheduledThreadPoolExecutor getScheduler() {
        return this.scheduler;
    }
    
    public void triggerTimersImmediately() {
        final Set<TimerEvent> timerEvents = new HashSet<TimerEvent>();
        for (final Runnable runnable : this.getScheduler().getQueue()) {
            if (runnable instanceof TimerEvent) {
                timerEvents.add((TimerEvent)runnable);
            }
        }
        for (final TimerEvent timerEvent : timerEvents) {
            final boolean needsToBeRun = this.getScheduler().remove(timerEvent);
            if (needsToBeRun) {
                timerEvent.run();
            }
        }
    }
    
    private static Ack makeAck(final Status status, final long pos) {
        return new Ack(status, pos);
    }
    
    private static Ack ackSuccess(final long pos) {
        return makeAck(Status.SUCCESS, pos);
    }
    
    private static Ack ackNeedMore(final long pos) {
        return makeAck(Status.NEEDMORE, pos);
    }
    
    public abstract boolean evalExpr(final MatcherContext p0, final int p1, final Object p2, final int p3, final Long p4);
    
    public abstract Object output(final MatcherContext p0, final Object p1);
    
    public abstract Object createNode(final MatcherContext p0, final int p1);
    
    public abstract int getRootNodeId();
    
    public abstract int getPrevEventIndex();
    
    public RecordKey makePartitionKey(final Object event) {
        return null;
    }
    
    public boolean checkDS(final int dsid, final int expected_ds) {
        return dsid != expected_ds;
    }
    
    public void doOutput(final Object o, final Position allEventsMergedPos) {
        this.cqtask.doOutput(Collections.singletonList(new DARecord(o, allEventsMergedPos)), Collections.emptyList());
    }
    
    public void doCleanup(final RecordKey key) {
        final MatcherPartition p = this.partitions.get(key);
        if (p != null) {
            this.partitions.remove(key);
        }
    }
    
    public PatternNode createAlternation(final MatcherContext ctx, final int nodeid, final String src, final int[] nodeids) {
        return new Alternation(ctx, nodeid, src, nodeids);
    }
    
    public PatternNode createAnchor(final MatcherContext ctx, final int nodeid, final String src, final int anchorid) {
        return new AnchorNode(ctx, nodeid, src);
    }
    
    public PatternNode createCondition(final MatcherContext ctx, final int nodeid, final String src, final int exprid) {
        return new Condition(ctx, nodeid, src, exprid);
    }
    
    public PatternNode createRepetition(final MatcherContext ctx, final int nodeid, final String src, final int subnodeid, final int min, final int max) {
        return new Repetition(ctx, nodeid, src, subnodeid, min, max);
    }
    
    public PatternNode createSequence(final MatcherContext ctx, final int nodeid, final String src, final int[] nodeids) {
        return new Sequence(ctx, nodeid, src, nodeids);
    }
    
    public PatternNode createVariable(final MatcherContext ctx, final int nodeid, final String src, final int exprid) {
        return new Variable(ctx, nodeid, src, exprid);
    }
    
    public Position getCheckpoint() {
        final PathManager result = new PathManager();
        for (final MatcherPartition p : this.partitions.values()) {
            final PathManager partitionCheckpoint = new PathManager();
            synchronized (p) {
                for (final PEvent pevent : p.eventBuffer.values()) {
                    partitionCheckpoint.mergeLowerPositions(pevent.position);
                }
            }
            String partitionKey = null;
            if (p.partKey != null) {
                partitionKey = p.partKey.toPartitionKey();
            }
            partitionCheckpoint.augment(this.cqtask.getMetaID(), partitionKey);
            result.mergeLowerPositions(partitionCheckpoint.toPosition());
        }
        return result.toPosition();
    }
    
    static {
        CQPatternMatcher.logger = Logger.getLogger((Class)CQPatternMatcher.class);
    }
    
    enum Status
    {
        NEEDMORE, 
        SUCCESS;
    }
    
    static class Ack
    {
        final Status status;
        final long pos;
        
        Ack(final Status status, final long pos) {
            this.status = status;
            this.pos = pos;
        }
        
        boolean advancedPos(final long prevpos) {
            return this.pos > prevpos;
        }
        
        boolean samePos(final long prevpos) {
            return this.pos == prevpos;
        }
        
        boolean samePosAndNeedMore(final long prevpos) {
            return this.samePos(prevpos) && this.needMore();
        }
        
        boolean needMore() {
            return this.status == Status.NEEDMORE;
        }
        
        boolean success() {
            return this.status == Status.SUCCESS;
        }
        
        @Override
        public String toString() {
            return "Ack(" + this.status + ", " + this.pos + ")";
        }
    }
    
    class MatcherPartition
    {
        private long curBufPos;
        private final RecordKey partKey;
        private final ConcurrentSkipListMap<Long, PEvent> eventBuffer;
        private MatcherContext ctx;
        private Object lastEvent;
        
        MatcherPartition(final RecordKey partKey) {
            this.curBufPos = 0L;
            this.eventBuffer = new ConcurrentSkipListMap<Long, PEvent>();
            this.partKey = partKey;
            this.ctx = new MatcherContext(this, 0L, false);
        }
        
        public void addEvent(final int streamID, final Object event, final Position position) {
            this.lastEvent = event;
            final long pos = this.curBufPos++;
            synchronized (this) {
                this.eventBuffer.put(pos, new PEvent(streamID, event, position));
                this.ctx = this.ctx.tryMatch();
                final long removeUpTo = this.ctx.startPos - CQPatternMatcher.this.getPrevEventIndex();
                this.eventBuffer.headMap(Long.valueOf(removeUpTo)).clear();
            }
        }
        
        public void stop() {
            this.ctx.close();
        }
        
        public void trace(final String string) {
            if (CQPatternMatcher.this.cqtask.isTracingExecution()) {
                final PrintStream out = CQPatternMatcher.this.cqtask.getTracingStream();
                out.println(string);
            }
        }
    }
    
    public static class PEvent
    {
        final int ds;
        final Object event;
        final Position position;
        
        public PEvent(final int ds, final Object event, final Position position) {
            this.ds = ds;
            this.event = event;
            this.position = position;
        }
        
        @Override
        public String toString() {
            return this.ds + "-> " + this.event;
        }
    }
    
    public class TimerEvent implements Runnable
    {
        final MatcherContext ctx;
        final int timerid;
        Future<?> task;
        private volatile boolean cancelled;
        
        TimerEvent(final MatcherContext ctx, final int timerid) {
            this.cancelled = false;
            this.ctx = ctx;
            this.timerid = timerid;
        }
        
        @Override
        public void run() {
            if (this.cancelled) {
                if (CQPatternMatcher.logger.isInfoEnabled()) {
                    CQPatternMatcher.logger.info((Object)("CQPatternMatcher TIMER for " + CQPatternMatcher.this.cqtask.getMetaName() + "canceled before running."));
                }
                return;
            }
            this.addToContext();
        }
        
        public void addToContext() {
            this.ctx.createTimerEvent(this);
        }
        
        void cancel() {
            this.cancelled = true;
            if (this.task != null) {
                this.task.cancel(true);
                if (this.task instanceof Runnable) {
                    CQPatternMatcher.this.getScheduler().remove((Runnable)this.task);
                }
            }
        }
    }
    
    public class MatcherContext
    {
        private final MatcherPartition owner;
        private final PatternNode root;
        private final long startPos;
        private long curPos;
        private Long lastAnchor;
        private final int contextId;
        private final Map<Integer, TimerEvent> timers;
        private final Map<Integer, List<Object>> vars;
        
        MatcherContext(final MatcherPartition owner, final long startPos, final boolean eval) {
            this.timers = new HashMap<Integer, TimerEvent>();
            this.vars = new HashMap<Integer, List<Object>>();
            this.owner = owner;
            this.contextId = CQPatternMatcher.this.nextCtxId.getAndIncrement();
            this.startPos = startPos;
            this.curPos = startPos;
            final int rootId = CQPatternMatcher.this.getRootNodeId();
            this.root = this.makeNode(rootId);
            if (eval) {
                final Ack ret = this.root.eval(this, this.curPos, false);
                if (!ret.success() && !ret.samePos(this.curPos)) {
                    ++this.curPos;
                }
            }
        }
        
        public MatcherContext tryMatch() {
            while (true) {
                final Ack ret = this.root.eval(this, this.curPos, false);
                if (ret.success()) {
                    this.close();
                    PathManager allEventsMergedPos = null;
                    final long fromPos = this.getRestartPosOnSuccess(ret.pos);
                    if (CQPatternMatcher.this.cqtask.recoveryIsEnabled()) {
                        final long eventsPos = fromPos - CQPatternMatcher.this.getPrevEventIndex();
                        final ConcurrentNavigableMap<Long, PEvent> removedMap = this.owner.eventBuffer.headMap(Long.valueOf(eventsPos));
                        allEventsMergedPos = new PathManager();
                        for (final PEvent pEvent : removedMap.values()) {
                            allEventsMergedPos.mergeLowerPositions(pEvent.position);
                        }
                    }
                    final Object o = CQPatternMatcher.this.output(this, this.owner.lastEvent);
                    final Position allEventsMergedPosPosition = (allEventsMergedPos != null) ? allEventsMergedPos.toPosition() : null;
                    CQPatternMatcher.this.doOutput(o, allEventsMergedPosPosition);
                    boolean reeval = true;
                    if (this.lastAnchor == null && this.owner.partKey != null) {
                        CQPatternMatcher.this.doCleanup(this.owner.partKey);
                        reeval = false;
                    }
                    return new MatcherContext(this.owner, fromPos, reeval);
                }
                if (ret.samePos(this.curPos)) {
                    this.close();
                    final long fromPos2 = this.getRestartPosOnFail();
                    return new MatcherContext(this.owner, fromPos2, true);
                }
                ++this.curPos;
                if (this.getEventAt(this.curPos) != null) {
                    continue;
                }
                return this;
            }
        }
        
        public void close() {
            for (final TimerEvent pte : this.timers.values()) {
                pte.cancel();
            }
        }
        
        public void createTimerEvent(final TimerEvent te) {
            this.owner.addEvent(-1, te, null);
        }
        
        public boolean createTimer(final int id, final long interval) {
            final TimerEvent te = new TimerEvent(this, id);
            te.task = CQPatternMatcher.this.getScheduler().schedule(te, interval, TimeUnit.MICROSECONDS);
            final TimerEvent pte = this.timers.put(id, te);
            if (pte != null) {
                pte.cancel();
            }
            return true;
        }
        
        public boolean stopTimer(final int timerid) {
            final TimerEvent pte = this.timers.get(timerid);
            if (pte != null) {
                pte.cancel();
            }
            return true;
        }
        
        public boolean waitTimer(final int timerid, final Object event) {
            if (event instanceof TimerEvent) {
                final TimerEvent te = (TimerEvent)event;
                if (this == te.ctx && te.timerid == timerid) {
                    return true;
                }
            }
            return false;
        }
        
        private long getRestartPosOnSuccess(final long endOfMatchedPattern) {
            return (this.lastAnchor == null) ? endOfMatchedPattern : this.lastAnchor;
        }
        
        private long getRestartPosOnFail() {
            return this.startPos + 1L;
        }
        
        public boolean eval(final int exprid, final Long pos) {
            boolean ret;
            if (pos != null) {
                final PEvent event = this.getEventAt(pos);
                ret = (event != null && CQPatternMatcher.this.evalExpr(this, exprid, event.event, event.ds, pos));
            }
            else {
                ret = CQPatternMatcher.this.evalExpr(this, exprid, null, 0, pos);
            }
            return ret;
        }
        
        private PEvent getEventAt(final long pos) {
            return this.owner.eventBuffer.get(pos);
        }
        
        public Object getEventAt(final Long pos, final int offset) {
            final PEvent pe = this.owner.eventBuffer.get(pos - offset);
            if (pe == null) {
                return null;
            }
            return pe.event;
        }
        
        public PatternNode makeNode(final int nodeid) {
            return (PatternNode)CQPatternMatcher.this.createNode(this, nodeid);
        }
        
        public void setAnchor(final long pos) {
            if (this.lastAnchor == null) {
                this.lastAnchor = pos;
            }
        }
        
        public void addVar(final int varid, final long pos) {
            final PEvent event = this.getEventAt(pos);
            if (event != null) {
                List<Object> var = this.vars.get(varid);
                if (var == null) {
                    var = new ArrayList<Object>();
                    this.vars.put(varid, var);
                }
                var.add(event.event);
            }
        }
        
        public Object getVar(final int varid, int index) {
            final List<Object> var = this.vars.get(varid);
            if (var != null) {
                if (index == -1) {
                    index = var.size() - 1;
                }
                try {
                    return var.get(index);
                }
                catch (IndexOutOfBoundsException ex) {}
            }
            return null;
        }
        
        public void trace(final String string) {
            this.owner.trace(string);
        }
    }
    
    public abstract static class PatternNode
    {
        private final int nodeid;
        private final String src;
        private long startpos;
        
        public PatternNode(final MatcherContext ctx, final int nodeid, final String src) {
            this.startpos = -1L;
            this.nodeid = nodeid;
            this.src = src;
        }
        
        public abstract Ack eval(final MatcherContext p0, final long p1, final boolean p2);
        
        public abstract boolean canAcceptMore();
        
        public void setStart(final long pos) {
            if (this.startpos == -1L) {
                this.startpos = pos;
            }
        }
        
        public long getStartPos() {
            return this.startpos;
        }
        
        public int getId() {
            return this.nodeid;
        }
        
        public String getSource() {
            return this.src;
        }
        
        @Override
        public String toString() {
            return this.nodeid + "(" + this.src + ")->" + this.startpos;
        }
    }
    
    public static class AnchorNode extends PatternNode
    {
        public AnchorNode(final MatcherContext ctx, final int nodeid, final String src) {
            super(ctx, nodeid, src);
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            ctx.setAnchor(pos);
            final Ack ack = ackSuccess(pos);
            return ack;
        }
        
        @Override
        public boolean canAcceptMore() {
            return false;
        }
    }
    
    public static class Condition extends PatternNode
    {
        private final int exprid;
        
        public Condition(final MatcherContext ctx, final int nodeid, final String src, final int exprid) {
            super(ctx, nodeid, src);
            this.exprid = exprid;
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            final boolean ok = ctx.eval(this.exprid, null);
            Ack ack;
            if (ok) {
                ack = ackSuccess(pos);
            }
            else {
                ack = ackNeedMore(pos);
            }
            return ack;
        }
        
        @Override
        public boolean canAcceptMore() {
            return false;
        }
    }
    
    public static class Variable extends PatternNode
    {
        private final int exprid;
        
        public Variable(final MatcherContext ctx, final int nodeid, final String src, final int exprid) {
            super(ctx, nodeid, src);
            this.exprid = exprid;
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            final boolean ok = ctx.eval(this.exprid, pos);
            Ack ack;
            if (ok) {
                ctx.addVar(this.exprid, pos);
                ctx.trace("matched " + this.getSource());
                ack = ackSuccess(pos + 1L);
            }
            else {
                ctx.trace("not matched " + this.getSource());
                ack = ackNeedMore(pos);
            }
            return ack;
        }
        
        @Override
        public boolean canAcceptMore() {
            return false;
        }
    }
    
    public static class Repetition extends PatternNode
    {
        private final int subnodeid;
        private final int min;
        private final int max;
        private final List<PatternNode> repeated;
        
        public Repetition(final MatcherContext ctx, final int nodeid, final String src, final int subnodeid, final int min, final int max) {
            super(ctx, nodeid, src);
            this.repeated = new ArrayList<PatternNode>();
            this.subnodeid = subnodeid;
            this.min = min;
            this.max = max;
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            if (!this.repeated.isEmpty()) {
                final PatternNode node = this.repeated.get(this.repeated.size() - 1);
                final Ack ret = node.eval(ctx, pos, greedy);
                if (ret.needMore()) {
                    return ret;
                }
                if (ret.advancedPos(pos)) {
                    if (this.thisNeeddMore()) {
                        return ackNeedMore(ret.pos);
                    }
                    return ret;
                }
            }
            if (!this.thisNeeddMore()) {
                if (greedy && this.canAcceptMore()) {
                    final PatternNode node = ctx.makeNode(this.subnodeid);
                    final Ack ret = node.eval(ctx, pos, greedy);
                    if (ret.success()) {
                        this.repeated.add(node);
                        return ret;
                    }
                }
                return ackSuccess(pos);
            }
            final PatternNode node = ctx.makeNode(this.subnodeid);
            final Ack ret = node.eval(ctx, pos, greedy);
            if (!ret.samePosAndNeedMore(pos)) {
                this.repeated.add(node);
            }
            if (ret.needMore() || this.thisNeeddMore()) {
                return ackNeedMore(ret.pos);
            }
            return ret;
        }
        
        @Override
        public boolean canAcceptMore() {
            return this.repeated.size() < this.max;
        }
        
        private boolean thisNeeddMore() {
            return this.repeated.size() < this.min;
        }
    }
    
    public static class Sequence extends PatternNode
    {
        private int cur;
        private final List<PatternNode> seq;
        
        public Sequence(final MatcherContext ctx, final int nodeid, final String src, final int[] seq) {
            super(ctx, nodeid, src);
            this.seq = new ArrayList<PatternNode>();
            this.cur = 0;
            for (final int id : seq) {
                this.seq.add(ctx.makeNode(id));
            }
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            if (this.cur >= this.seq.size()) {
                return ackSuccess(pos);
            }
            final PatternNode node = this.seq.get(this.cur);
            final Ack ret = node.eval(ctx, pos, greedy);
            if (ret.needMore()) {
                if (!ret.samePosAndNeedMore(pos)) {
                    return ret;
                }
                return this.tryRecompute(ctx, pos);
            }
            else {
                ++this.cur;
                if (!ret.advancedPos(pos)) {
                    return this.eval(ctx, pos, greedy);
                }
                if (this.cur == this.seq.size()) {
                    return ret;
                }
                return ackNeedMore(ret.pos);
            }
        }
        
        private int findAcceptMore(final int lastcompletednode, final long lastconsumedpos) {
            int canAcceptMore = -1;
            for (int i = lastcompletednode; i >= 0; --i) {
                final PatternNode node = this.seq.get(i);
                if (node.canAcceptMore()) {
                    canAcceptMore = i;
                }
                if (node.getStartPos() != lastconsumedpos) {
                    break;
                }
            }
            return canAcceptMore;
        }
        
        Ack tryRecompute(final MatcherContext ctx, final long pos) {
            int i = this.findAcceptMore(this.cur - 1, pos - 1L);
            if (i < 0) {
                return ackNeedMore(pos);
            }
            long cpos = pos;
            Ack ret;
            while (true) {
                final PatternNode node = this.seq.get(i);
                ret = node.eval(ctx, cpos, true);
                if (!ret.success() || ++i > this.cur) {
                    break;
                }
                cpos = ret.pos;
            }
            this.cur = i;
            return ret;
        }
        
        @Override
        public boolean canAcceptMore() {
            return this.findAcceptMore(this.cur, this.seq.get(this.cur).getStartPos()) >= 0;
        }
    }
    
    public static class Alternation extends PatternNode
    {
        private final List<PatternNode> alternatives;
        private List<PatternNode> left;
        
        public Alternation(final MatcherContext ctx, final int nodeid, final String src, final int[] alternatives) {
            super(ctx, nodeid, src);
            this.alternatives = new ArrayList<PatternNode>();
            for (final int id : alternatives) {
                this.alternatives.add(ctx.makeNode(id));
            }
            this.left = this.alternatives;
        }
        
        @Override
        public Ack eval(final MatcherContext ctx, final long pos, final boolean greedy) {
            this.setStart(pos);
            boolean consumed = false;
            final List<PatternNode> needmore = new ArrayList<PatternNode>();
            final List<PatternNode> variants = greedy ? this.alternatives : this.left;
            for (final PatternNode n : variants) {
                final Ack ret = n.eval(ctx, pos, greedy);
                if (ret.needMore()) {
                    needmore.add(n);
                }
                if (ret.advancedPos(pos)) {
                    consumed = true;
                }
            }
            this.left = needmore;
            if (needmore.size() == this.alternatives.size()) {
                return ackNeedMore(consumed ? (pos + 1L) : pos);
            }
            return ackSuccess(consumed ? (pos + 1L) : pos);
        }
        
        @Override
        public boolean canAcceptMore() {
            for (final PatternNode n : this.alternatives) {
                if (n.canAcceptMore()) {
                    return true;
                }
            }
            return false;
        }
    }
}
