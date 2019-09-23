package com.datasphere.runtime.window;

import com.datasphere.runtime.meta.*;
import java.util.concurrent.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

public class SlidingPolicy
{
    private final int rowCount;
    private final CmpAttrs attrComparator;
    private final long timeInterval;
    private final OutputBufferFactory factory;
    
    private SlidingPolicy(final IntervalPolicy.Kind slideKind, final int count, final long interval, final CmpAttrs sAttrComparator) {
        this.rowCount = count;
        this.timeInterval = interval;
        this.attrComparator = sAttrComparator;
        this.factory = this.createOutputBuffer(slideKind);
    }
    
    private OutputBufferFactory createCountBuffer() {
        return new OutputBufferFactory() {
            @Override
            public OutputBuffer create(final BufWindow wnd) {
                final OutputBuffer b = new OutputBuffer(wnd) {
                    @Override
                    public void update(final Snapshot snp, final IBatch added, final IBatch removed) {
                        this.setSnapshot(snp);
                        for (final Object e : removed) {
                            this.addRemovedEvent((DARecord)e);
                        }
                        for (final Object e : added) {
                            this.addEvent((DARecord)e);
                            if (this.countFull()) {
                                this.doJump();
                            }
                        }
                    }
                };
                b.setTimer(SlidingPolicy.this.createNoTimerPolicy());
                return b;
            }
        };
    }
    
    private OutputBufferFactory createAttrBuffer() {
        return new OutputBufferFactory() {
            @Override
            public OutputBuffer create(final BufWindow wnd) {
                final OutputBuffer b = new OutputBuffer(wnd) {
                    @Override
                    public void update(final Snapshot snp, final IBatch added, final IBatch removed) {
                        this.setSnapshot(snp);
                        for (final Object e : removed) {
                            this.addRemovedEvent((DARecord)e);
                        }
                        for (final Object ev : added) {
                            try {
                                final DARecord firstInBuffer = this.getFirstEvent();
                                if (!this.inRange(firstInBuffer, (DARecord)ev)) {
                                    this.doJump();
                                }
                            }
                            catch (NoSuchElementException ex) {}
                            this.addEvent((DARecord)ev);
                        }
                    }
                };
                b.setTimer(SlidingPolicy.this.createNoTimerPolicy());
                return b;
            }
        };
    }
    
    private OutputBufferFactory createTimeBuffer() {
        return new OutputBufferFactory() {
            @Override
            public OutputBuffer create(final BufWindow wnd) {
                final OutputBuffer b = new OutputBuffer(wnd) {
                    @Override
                    public void update(final Snapshot snp, final IBatch added, final IBatch removed) {
                        this.setSnapshot(snp);
                        for (final Object e : removed) {
                            this.addRemovedEvent((DARecord)e);
                        }
                        this.addEvents((IBatch<DARecord>)added);
                    }
                    
                    @Override
                    void removeAll() throws Exception {
                        this.doJump();
                    }
                };
                b.setTimer(SlidingPolicy.this.createTimePolicy(wnd, b));
                return b;
            }
        };
    }
    
    private OutputBufferFactory createOutputBuffer(final IntervalPolicy.Kind slideKind) {
        switch (slideKind) {
            case ATTR:
            case TIME_ATTR: {
                return this.createAttrBuffer();
            }
            case COUNT:
            case TIME_COUNT: {
                return this.createCountBuffer();
            }
            case TIME: {
                return this.createTimeBuffer();
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }
    
    private WTimerPolicy createNoTimerPolicy() {
        return new WTimerPolicy() {
            @Override
            public void startTimer() {
            }
            
            @Override
            public void stopTimer() {
            }
            
            @Override
            public void run() {
            }
        };
    }
    
    private WTimerPolicy createTimePolicy(final BufWindow wnd, final OutputBuffer buffer) {
        return new WTimerPolicy() {
            Future<?> task;
            
            @Override
            public void stopTimer() {
                if (this.task != null) {
                    this.task.cancel(true);
                    this.task = null;
                }
            }
            
            @Override
            public void startTimer() {
                this.task = wnd.getScheduler().scheduleAtFixedRate(this, SlidingPolicy.this.timeInterval, SlidingPolicy.this.timeInterval, TimeUnit.NANOSECONDS);
            }
            
            @Override
            public void run() {
                try {
                    buffer.removeAll();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
    
    public OutputBuffer createOutputBuffer(final BufWindow wnd) {
        final OutputBuffer b = this.factory.create(wnd);
        b.start();
        return b;
    }
    
    public static SlidingPolicy create(final IntervalPolicy.Kind slideKind, final Integer sRowCount, final CmpAttrs sAttrComparator, final Long sTimeInterval) {
        final int count = (sRowCount == null) ? 0 : sRowCount;
        final long interval = (sTimeInterval == null) ? 0L : sTimeInterval;
        return new SlidingPolicy(slideKind, count, interval, sAttrComparator);
    }
    
    public abstract class OutputBuffer
    {
        private final BufWindow owner;
        private WTimerPolicy timer;
        private List<DARecord> addedBuffer;
        private List<DARecord> removedBuffer;
        private Snapshot currSnapshot;
        
        public abstract void update(final Snapshot p0, final IBatch p1, final IBatch p2);
        
        private OutputBuffer(final BufWindow owner) {
            this.addedBuffer = new ArrayList<DARecord>();
            this.removedBuffer = new ArrayList<DARecord>();
            this.owner = owner;
        }
        
        void setTimer(final WTimerPolicy timer) {
            this.timer = timer;
        }
        
        void start() {
            this.timer.startTimer();
        }
        
        void stop() {
            this.timer.stopTimer();
        }
        
        void removeAll() throws Exception {
            throw new UnsupportedOperationException();
        }
        
        final void addEvents(final IBatch<DARecord> added) {
            for (final DARecord e : added) {
                this.addEvent(e);
            }
        }
        
        final void setSnapshot(final Snapshot snp) {
            this.currSnapshot = snp;
        }
        
        final void addRemovedEvent(final DARecord e) {
            this.removedBuffer.add(e);
        }
        
        final boolean countFull() {
            return this.getSize() == SlidingPolicy.this.rowCount;
        }
        
        final boolean inRange(final DARecord a, final DARecord b) {
            return SlidingPolicy.this.attrComparator.inRange(a, b);
        }
        
        final void addEvent(final DARecord e) {
            this.addedBuffer.add(e);
        }
        
        final DARecord getFirstEvent() throws NoSuchElementException {
            try {
                return this.addedBuffer.get(0);
            }
            catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }
        
        final int getSize() {
            return this.addedBuffer.size();
        }
        
        void doJump() {
            final List<DARecord> newrec = this.addedBuffer;
            final List<DARecord> oldrec = this.removedBuffer;
            this.removedBuffer = new ArrayList<DARecord>();
            this.addedBuffer = new ArrayList<DARecord>();
            this.owner.getFactory().receive(this.owner.getPartKey(), this.currSnapshot, (IBatch)Batch.asBatch(newrec), (IBatch)Batch.asBatch(oldrec));
        }
    }
    
    private interface WTimerPolicy extends Runnable
    {
        void startTimer();
        
        void stopTimer();
    }
    
    private interface OutputBufferFactory
    {
        OutputBuffer create(final BufWindow p0);
    }
}
