package com.datasphere.runtime.containers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.RecordKey;
import com.datasphere.uuid.UUID;

public abstract class TaskEvent implements ITaskEvent
{
    public static final int HAS_DATA = 1;
    public static final int RESET_AGGREGATES = 2;
    private static final long serialVersionUID = 5836478948970080038L;
    private static Logger logger;
    private UUID queryID;
    private boolean isLagRecord;
    private LagMarker lagMarker;
    
    public TaskEvent() {
        this.queryID = null;
        this.isLagRecord = false;
        this.lagMarker = null;
    }
    
    public void setLagRecord(final boolean lagRecord) {
        this.isLagRecord = lagRecord;
    }
    
    public LagMarker getLagMarker() {
        return this.lagMarker;
    }
    
    public void setLagMarker(final LagMarker lagMarker) {
        this.lagMarker = lagMarker;
    }
    
    public boolean isLagRecord() {
        return this.isLagRecord;
    }
    
    public boolean canBeLagEvent() {
        return true;
    }
    
    public boolean snapshotUpdate() {
        return this.batch().isEmpty() && this.removedBatch().isEmpty();
    }
    
    public IBatch batch() {
        return (IBatch)Batch.emptyBatch();
    }
    
    public IBatch filterBatch(final int indexID, final RecordKey key) {
        return (IBatch)Batch.emptyBatch();
    }
    
    public IBatch removedBatch() {
        return (IBatch)Batch.emptyBatch();
    }
    
    public IRange snapshot() {
        return (IRange)Range.emptyRange();
    }
    
    public int getFlags() {
        return 1;
    }
    
    public UUID getQueryID() {
        return this.queryID;
    }
    
    public void setQueryID(final UUID queryID) {
        this.queryID = queryID;
    }
    
    @Override
    public String toString() {
        return "TE(" + this.batch() + " " + this.removedBatch() + " " + this.snapshot() + ")";
    }
    
    public static TaskEvent createWindowEvent(final IBatch added, final IBatch removed, final IRange range) {
        return new TaskEvent() {
            private static final long serialVersionUID = 5776239061208773283L;
            IBatch xadded = added;
            IBatch xremoved = removed;
            IRange xrange = range;
            
            @Override
            public IBatch batch() {
                return this.xadded;
            }
            
            @Override
            public IBatch removedBatch() {
                return this.xremoved;
            }
            
            @Override
            public IRange snapshot() {
                return this.xrange;
            }
            
            public void write(final Kryo kryo, final Output output) {
                kryo.writeClassAndObject(output, (Object)this.xadded);
                kryo.writeClassAndObject(output, (Object)this.xremoved);
                kryo.writeClassAndObject(output, (Object)this.xrange);
            }
            
            public void read(final Kryo kryo, final Input input) {
                this.xadded = (IBatch)kryo.readClassAndObject(input);
                this.xremoved = (IBatch)kryo.readClassAndObject(input);
                this.xrange = (IRange)kryo.readClassAndObject(input);
            }
        };
    }
    
    public static TaskEvent createStreamEvent(final List<DARecord> added) {
        return new TaskEvent() {
            private static final long serialVersionUID = 5260812394346021244L;
            List<DARecord> xadded = added;
            
            public Batch batch() {
                return Batch.asBatch(this.xadded);
            }
            
            public void write(final Kryo kr, final Output output) {
                if (added != null && this.xadded.size() > 0) {
                    if (added.size() == 1) {
                        output.writeByte(1);
                        kr.writeObject(output, (Object)this.xadded.get(0));
                    }
                    else {
                        output.writeByte(2);
                        kr.writeClassAndObject(output, (Object)added);
                    }
                }
                else {
                    output.writeByte(0);
                }
            }
            
            public void read(final Kryo kr, final Input input) {
                final byte hasAdded = input.readByte();
                if (hasAdded == 0) {
                    this.xadded = (List<DARecord>)Collections.EMPTY_LIST;
                }
                else if (hasAdded == 1) {
                    final DARecord item = (DARecord)kr.readObject(input, (Class)DARecord.class);
                    this.xadded = Collections.singletonList(item);
                }
                else {
                    this.xadded = (List<DARecord>)kr.readClassAndObject(input);
                }
            }
        };
    }
    
    private static void writeList(final List<DARecord> data, final Kryo kr, final Output output) {
        if (data != null && data.size() > 0) {
            output.writeByte((byte)1);
            final int size = data.size();
            output.writeInt(size);
            for (final DARecord aData : data) {
                kr.writeObject(output, (Object)aData);
            }
        }
        else {
            output.writeByte((byte)0);
        }
    }
    
    private static List<DARecord> readList(final Kryo kr, final Input input) {
        final byte isEmpty = input.readByte();
        if (isEmpty == 0) {
            return Collections.emptyList();
        }
        final int size = input.readInt();
        assert size >= 0;
        final List<DARecord> ret = new ArrayList<DARecord>();
        for (int i = 0; i < size; ++i) {
            final DARecord item = (DARecord)kr.readObject(input, (Class)DARecord.class);
            ret.add(item);
        }
        return ret;
    }
    
    public static TaskEvent createStreamEvent(final List<DARecord> added, final List<DARecord> removed, final boolean isStateful) {
        return new TaskEvent() {
            private static final long serialVersionUID = 8121755941701609319L;
            List<DARecord> xadded = added;
            List<DARecord> xremoved = removed;
            boolean stateful = isStateful;
            
            public Batch batch() {
                return Batch.asBatch(this.xadded);
            }
            
            @Override
            public IBatch removedBatch() {
                return (IBatch)Batch.asBatch(this.xremoved);
            }
            
            @Override
            public IRange snapshot() {
                if (this.stateful) {
                    return (IRange)Range.createRange(null, added);
                }
                return (IRange)Range.emptyRange();
            }
            
            public void write(final Kryo kr, final Output output) {
                output.writeBoolean(this.stateful);
                writeList(this.xadded, kr, output);
                writeList(this.xremoved, kr, output);
                final UUID test = this.getQueryID();
                if (test == null) {
                    output.writeByte((byte)0);
                }
                else {
                    output.writeByte((byte)1);
                    output.writeString(test.getUUIDString());
                }
                if (this.isLagRecord()) {
                    output.writeBoolean(true);
                    final LagMarker lagMarker = this.getLagMarker();
                    lagMarker.write(kr, output);
                }
                else {
                    output.writeBoolean(false);
                }
            }
            
            public void read(final Kryo kr, final Input input) {
                this.stateful = input.readBoolean();
                this.xadded = readList(kr, input);
                this.xremoved = readList(kr, input);
                final byte result = input.readByte();
                if (result == 1) {
                    final String uuidString = input.readString();
                    if (uuidString != null) {
                        this.setQueryID(new UUID(uuidString));
                    }
                }
                final boolean isLagRecord = input.readBoolean();
                if (isLagRecord) {
                    final LagMarker lagMarker = new LagMarker();
                    lagMarker.read(kr, input);
                    this.setLagMarker(lagMarker);
                    this.setLagRecord(true);
                }
                else {
                    this.setLagRecord(false);
                }
            }
        };
    }
    
    public static TaskEvent createWindowStateEvent(final Range snapshot) {
        return new TaskEvent() {
            private static final long serialVersionUID = 3621467538164019186L;
            Range xsnapshot = snapshot;
            
            @Override
            public IBatch batch() {
                return this.xsnapshot.all();
            }
            
            @Override
            public IBatch filterBatch(final int indexID, final RecordKey key) {
                return this.xsnapshot.lookup(indexID, key);
            }
            
            public Range snapshot() {
                return this.xsnapshot;
            }
            
            public void write(final Kryo kryo, final Output output) {
                kryo.writeClassAndObject(output, (Object)this.xsnapshot);
            }
            
            public void read(final Kryo kryo, final Input input) {
                this.xsnapshot = (Range)kryo.readClassAndObject(input);
            }
        };
    }
    
    public static TaskEvent createWAStoreQueryEvent(final Range snapshot) {
        return new TaskEvent() {
            private static final long serialVersionUID = 4601388050386280658L;
            Range xsnapshot = snapshot;
            
            @Override
            public IBatch batch() {
                return this.xsnapshot.all();
            }
            
            @Override
            public IBatch filterBatch(final int indexID, final RecordKey key) {
                return this.xsnapshot.lookup(indexID, key);
            }
            
            public Range snapshot() {
                return this.xsnapshot;
            }
            
            public void write(final Kryo kryo, final Output output) {
                kryo.writeClassAndObject(output, (Object)this.xsnapshot);
            }
            
            public void read(final Kryo kryo, final Input input) {
                this.xsnapshot = (Range)kryo.readClassAndObject(input);
            }
            
            @Override
            public int getFlags() {
                return 3;
            }
        };
    }
    
    public static TaskEvent createStreamEventWithNewDARecords(final ITaskEvent event) {
        final List<DARecord> added = new ArrayList<DARecord>();
        for (final DARecord original : event.batch()) {
            try {
                final DARecord newInstance = (DARecord)original.getClass().newInstance();
                newInstance.initValues(original);
                added.add(newInstance);
            }
            catch (InstantiationException e) {
                TaskEvent.logger.error((Object)e);
            }
            catch (IllegalAccessException e2) {
                TaskEvent.logger.error((Object)e2);
            }
        }
        final List<DARecord> removed = new ArrayList<DARecord>();
        for (final DARecord original2 : event.removedBatch()) {
            try {
                final DARecord newInstance2 = (DARecord)original2.getClass().newInstance();
                newInstance2.initValues(original2);
                removed.add(newInstance2);
            }
            catch (InstantiationException e3) {
                TaskEvent.logger.error((Object)e3);
            }
            catch (IllegalAccessException e4) {
                TaskEvent.logger.error((Object)e4);
            }
        }
        return new TaskEvent() {
            List<DARecord> _added = added;
            List<DARecord> _removed = removed;
            ITaskEvent _event = event;
            private static final long serialVersionUID = -1246997384610275229L;
            
            public Batch batch() {
                return Batch.asBatch(this._added);
            }
            
            @Override
            public IBatch removedBatch() {
                return (IBatch)Batch.asBatch(this._removed);
            }
            
            @Override
            public IRange snapshot() {
                return this._event.snapshot();
            }
            
            public void write(final Kryo kr, final Output output) {
                if (this._added != null && this._added.size() > 0) {
                    if (this._added.size() == 1) {
                        output.writeByte(1);
                        kr.writeObject(output, (Object)this._added.get(0));
                    }
                    else {
                        output.writeByte(2);
                        kr.writeClassAndObject(output, (Object)this._added);
                    }
                }
                else {
                    output.writeByte(0);
                }
                if (this._removed != null && this._removed.size() > 0) {
                    if (this._removed.size() == 1) {
                        output.writeByte(1);
                        kr.writeObject(output, (Object)this._removed.get(0));
                    }
                    else {
                        output.writeByte(2);
                        kr.writeClassAndObject(output, (Object)this._removed);
                    }
                }
                else {
                    output.writeByte(0);
                }
                if (this._event != null) {
                    output.writeByte(1);
                    kr.writeClassAndObject(output, (Object)this._event);
                }
            }
            
            public void read(final Kryo kr, final Input input) {
                this._added = new ArrayList<DARecord>();
                this._removed = new ArrayList<DARecord>();
                final byte hasAdded = input.readByte();
                if (hasAdded != 0) {
                    if (hasAdded == 1) {
                        final DARecord item = (DARecord)kr.readObject(input, (Class)DARecord.class);
                        this._added.add(item);
                    }
                    else {
                        this._added.addAll((Collection<? extends DARecord>)kr.readClassAndObject(input));
                    }
                }
                final byte hasRemoved = input.readByte();
                if (hasRemoved != 0) {
                    if (hasRemoved == 1) {
                        final DARecord item2 = (DARecord)kr.readObject(input, (Class)DARecord.class);
                        this._removed.add(item2);
                    }
                    else {
                        this._removed.addAll((Collection<? extends DARecord>)kr.readClassAndObject(input));
                    }
                }
                final byte hasEvent = input.readByte();
                if (hasEvent != 0) {
                    this._event = (ITaskEvent)kr.readClassAndObject(input);
                }
            }
        };
    }
    
    static {
        TaskEvent.logger = Logger.getLogger((Class)TaskEvent.class);
    }
}
