package com.datasphere.runtime.window;

import org.apache.log4j.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;

public abstract class BufWindowFactory
{
    protected static Logger logger;
    private static AtomicInteger next_window_id;
    protected final int id;
    private volatile BufWindowSub sub;
    private final WindowPolicy policy;
    
    private BufWindowFactory(final WindowPolicy wp) {
        this.id = BufWindowFactory.next_window_id.getAndIncrement();
        this.policy = wp;
    }
    
    public SlidingPolicy.OutputBuffer createOutputBuffer(final BufWindow wnd) {
        final SlidingPolicy slidingPolicy = this.policy.getSlidingPolicy();
        return (slidingPolicy == null) ? null : slidingPolicy.createOutputBuffer(wnd);
    }
    
    public WindowPolicy getPolicy() {
        return this.policy;
    }
    
    public void addSub(final BufWindowSub sub) {
        this.sub = sub;
    }
    
    public void removeAllSubsribers() {
        this.sub = null;
    }
    
    public abstract BufWindow createWindow(final BufferManager p0);
    
    @Override
    public String toString() {
        return "bufwindow(" + this.id + ", " + this.getClass().getCanonicalName() + ", " + this.policy + ")";
    }
    
    @Override
    public int hashCode() {
        return this.id;
    }
    
    @Override
    public boolean equals(final Object o) {
        return o instanceof BufWindowFactory && ((BufWindowFactory)o).id == this.id;
    }
    
    void receive(final RecordKey partKey, final Collection<DARecord> snapshot, final IBatch newEntries, final IBatch oldEntries) {
        assert oldEntries.size() + newEntries.size() > 0;
        final BufWindowSub s = this.sub;
        if (s != null) {
            s.receive(partKey, snapshot, newEntries, oldEntries);
        }
    }
    
    private static BufWindowFactory create(final WindowPolicy wp) {
        switch (wp.getKind()) {
            case ATTR: {
                if (wp.isJumping()) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new JumpingAttributeWindow(owner);
                        }
                    };
                }
                return new BufWindowFactory(wp) {
                    @Override
                    public BufWindow createWindow(final BufferManager owner) {
                        return new SlidingAttributeWindow(owner);
                    }
                };
            }
            case COUNT: {
                if (wp.isJumping()) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new JumpingCountWindow(owner);
                        }
                    };
                }
                return new BufWindowFactory(wp) {
                    @Override
                    public BufWindow createWindow(final BufferManager owner) {
                        return new SlidingCountWindow(owner);
                    }
                };
            }
            case TIME: {
                if (wp.isJumping()) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new JumpingTimeWindow(owner);
                        }
                    };
                }
                return new BufWindowFactory(wp) {
                    @Override
                    public BufWindow createWindow(final BufferManager owner) {
                        return new SlidingTimeWindow(owner);
                    }
                };
            }
            case TIME_ATTR: {
                if (wp.isJumping()) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new JumpingTimeAttrWindow(owner);
                        }
                    };
                }
                return new BufWindowFactory(wp) {
                    @Override
                    public BufWindow createWindow(final BufferManager owner) {
                        return new SlidingTimeAttrWindow(owner);
                    }
                };
            }
            case TIME_COUNT: {
                if (!wp.isJumping()) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new SlidingTimeCountWindow(owner);
                        }
                    };
                }
                if (wp.getRowCount() == -1) {
                    return new BufWindowFactory(wp) {
                        @Override
                        public BufWindow createWindow(final BufferManager owner) {
                            return new JumpingSessionWindow(owner);
                        }
                    };
                }
                return new BufWindowFactory(wp) {
                    @Override
                    public BufWindow createWindow(final BufferManager owner) {
                        return new JumpingTimeCountWindow(owner);
                    }
                };
            }
            default: {
                throw new IllegalArgumentException("unknow kind of window " + wp.getKind());
            }
        }
    }
    
    public static BufWindowFactory create(final IntervalPolicy.Kind kind, final boolean isJumping, final Integer rowCount, final CmpAttrs attrComparator, final Long timeInterval, final SlidingPolicy slidingPolicy) {
        final WindowPolicy wp = WindowPolicy.makePolicy(kind, isJumping, rowCount, attrComparator, timeInterval, slidingPolicy);
        return create(wp);
    }
    
    private static SlidingPolicy createSlidingPolicy(final MetaInfo.Window wi, final BaseServer srv) {
        if (wi.slidePolicy == null) {
            return null;
        }
        final IntervalPolicy ip = wi.windowLen;
        final IntervalPolicy sp = wi.slidePolicy;
        final IntervalPolicy.Kind kind = sp.getKind();
        final IntervalPolicy.AttrBasedPolicy iap = ip.getAttrPolicy();
        final IntervalPolicy.AttrBasedPolicy isp = sp.getAttrPolicy();
        final CmpAttrs sAttrComparator = (kind == IntervalPolicy.Kind.ATTR || kind == IntervalPolicy.Kind.TIME_ATTR) ? AttrExtractor.createAttrComparator(iap.getAttrName(), isp.getAttrValueRange() / 1000L, wi, srv) : null;
        final Integer sRowCount = (kind == IntervalPolicy.Kind.COUNT || kind == IntervalPolicy.Kind.TIME_COUNT) ? sp.getCountPolicy().getCountInterval() : null;
        final Long sTimeInterval = (kind == IntervalPolicy.Kind.TIME || kind == IntervalPolicy.Kind.TIME_ATTR) ? TimeUnit.MICROSECONDS.toNanos(sp.getTimePolicy().getTimeInterval()) : null;
        return SlidingPolicy.create(kind, sRowCount, sAttrComparator, sTimeInterval);
    }
    
    public static BufWindowFactory create(final MetaInfo.Window wi, final BaseServer srv, final BufWindowSub sub) {
        final IntervalPolicy ip = wi.windowLen;
        final boolean isJumping = wi.jumping;
        final IntervalPolicy.Kind kind = ip.getKind();
        final CmpAttrs attrComparator = (kind == IntervalPolicy.Kind.ATTR || kind == IntervalPolicy.Kind.TIME_ATTR) ? AttrExtractor.createAttrComparator(wi, srv) : null;
        final Integer rowCount = (kind == IntervalPolicy.Kind.COUNT || kind == IntervalPolicy.Kind.TIME_COUNT) ? ip.getCountPolicy().getCountInterval() : null;
        final Long timeInterval = (kind == IntervalPolicy.Kind.TIME || kind == IntervalPolicy.Kind.TIME_ATTR || kind == IntervalPolicy.Kind.TIME_COUNT) ? TimeUnit.MICROSECONDS.toNanos(ip.getTimePolicy().getTimeInterval()) : null;
        final SlidingPolicy slidingPolicy = createSlidingPolicy(wi, srv);
        final BufWindowFactory fac = create(kind, isJumping, rowCount, attrComparator, timeInterval, slidingPolicy);
        fac.addSub(sub);
        return fac;
    }
    
    public static BufWindowFactory createSlidingCount(final int row_count) {
        return create(IntervalPolicy.Kind.COUNT, false, row_count, null, null, null);
    }
    
    public static BufWindowFactory createSlidingAttribute(final CmpAttrs attrComparator) {
        return create(IntervalPolicy.Kind.ATTR, false, null, attrComparator, null, null);
    }
    
    public static BufWindowFactory createSlidingTime(final long time_interval) {
        return create(IntervalPolicy.Kind.TIME, false, null, null, time_interval, null);
    }
    
    public static BufWindowFactory createSlidingTimeCount(final long time_interval, final int row_count) {
        return create(IntervalPolicy.Kind.TIME_COUNT, false, row_count, null, time_interval, null);
    }
    
    public static BufWindowFactory createSlidingTimeAttr(final long time_interval, final CmpAttrs attrComparator) {
        return create(IntervalPolicy.Kind.TIME_ATTR, false, null, attrComparator, time_interval, null);
    }
    
    public static BufWindowFactory createJumpingCount(final int row_count) {
        return create(IntervalPolicy.Kind.COUNT, true, row_count, null, null, null);
    }
    
    public static BufWindowFactory createJumpingAttribute(final CmpAttrs attrComparator) {
        return create(IntervalPolicy.Kind.ATTR, true, null, attrComparator, null, null);
    }
    
    public static BufWindowFactory createJumpingTime(final long time_interval) {
        return create(IntervalPolicy.Kind.TIME, true, null, null, time_interval, null);
    }
    
    public static BufWindowFactory createJumpingTimeCount(final long time_interval, final int row_count) {
        return create(IntervalPolicy.Kind.TIME_COUNT, true, row_count, null, time_interval, null);
    }
    
    public static BufWindowFactory createJumpingTimeAttr(final long time_interval, final CmpAttrs attrComparator) {
        return create(IntervalPolicy.Kind.TIME_ATTR, true, null, attrComparator, time_interval, null);
    }
    
    static {
        BufWindowFactory.logger = Logger.getLogger((Class)BufWindowFactory.class);
        BufWindowFactory.next_window_id = new AtomicInteger(1);
    }
}
