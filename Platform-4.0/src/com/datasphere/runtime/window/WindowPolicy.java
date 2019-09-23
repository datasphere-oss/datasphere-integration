package com.datasphere.runtime.window;

import com.datasphere.runtime.meta.*;

public class WindowPolicy
{
    private final Integer rowCount;
    private final CmpAttrs attrComparator;
    private final Long timeInterval;
    private final boolean isJumping;
    private final IntervalPolicy.Kind kind;
    private final SlidingPolicy slidingPolicy;
    
    private WindowPolicy(final IntervalPolicy.Kind kind, final boolean isJumping, final Integer rowCount, final CmpAttrs attrComparator, final Long timeInterval, final SlidingPolicy slidingPolicy) {
        this.kind = kind;
        this.isJumping = isJumping;
        this.rowCount = rowCount;
        this.attrComparator = attrComparator;
        this.timeInterval = timeInterval;
        this.slidingPolicy = slidingPolicy;
    }
    
    public Integer getRowCount() {
        return this.rowCount;
    }
    
    public CmpAttrs getComparator() {
        return this.attrComparator;
    }
    
    public Long getTimeInterval() {
        return this.timeInterval;
    }
    
    public boolean isJumping() {
        return this.isJumping;
    }
    
    public IntervalPolicy.Kind getKind() {
        return this.kind;
    }
    
    public SlidingPolicy getSlidingPolicy() {
        return this.slidingPolicy;
    }
    
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(this.kind + " " + (this.isJumping ? "jumping" : "") + " ");
        if (this.rowCount != null) {
            sb.append("rowCount:" + this.rowCount + " ");
        }
        if (this.timeInterval != null) {
            sb.append("timeInterval:" + this.timeInterval + " ");
        }
        if (this.attrComparator != null) {
            sb.append("comparator:" + this.attrComparator + " ");
        }
        return sb.toString();
    }
    
    public static WindowPolicy makePolicy(final IntervalPolicy.Kind kind, final boolean isJumping, final Integer rowCount, final CmpAttrs attrComparator, final Long timeInterval, final SlidingPolicy slidingPolicy) {
        return new WindowPolicy(kind, isJumping, rowCount, attrComparator, timeInterval, slidingPolicy);
    }
}
