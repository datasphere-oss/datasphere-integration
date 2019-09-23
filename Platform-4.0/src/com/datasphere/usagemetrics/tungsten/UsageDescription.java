package com.datasphere.usagemetrics.tungsten;

import com.datasphere.usagemetrics.cache.*;

class UsageDescription
{
    public final String sourceName;
    public final UsageMetrics usageMetrics;
    private final UsageFormat format;
    private static final int PETABYTE_EXPONENT = 50;
    private static final int KILOBYTE_EXPONENT = 10;
    private static final long MAXIMUM_SMALL_VALUE;
    
    UsageDescription(final UsageFormat format, final String sourceName, final UsageMetrics usageMetrics) {
        this.format = format;
        this.sourceName = sourceName;
        this.usageMetrics = usageMetrics;
    }
    
    long getUsageValue() {
        return (this.usageMetrics == null) ? 0L : this.usageMetrics.sourceBytes;
    }
    
    String getValueString() {
        final long value = this.getUsageValue();
        if (value <= UsageDescription.MAXIMUM_SMALL_VALUE) {
            return makeSmallValueString(value);
        }
        return this.makeLargeValueString(value);
    }
    
    private static String makeSmallValueString(final long value) {
        final String unitName = UsageFormat.getUnitName(0);
        return String.format("%4d  %s", value, unitName);
    }
    
    private String makeLargeValueString(final long value) {
        int scale;
        for (scale = 50; value < powerOfTwo(scale); scale -= 10) {}
        final String unitName = UsageFormat.getUnitName(scale);
        final String byteCount = this.getFullByteCount(value);
        return getScaledValue(value, scale, unitName, byteCount);
    }
    
    private static String getScaledValue(final long value, final int scale, final String unit, final String byteCount) {
        final long divisor = powerOfTwo(scale);
        return String.format("%6.1f%s%s", value / divisor, unit, byteCount);
    }
    
    private static long powerOfTwo(final int exponent) {
        return 1L << exponent;
    }
    
    private String getFullByteCount(final long value) {
        return String.format(this.format.valueFormat, value);
    }
    
    @Override
    public String toString() {
        this.calculateMaxLengths();
        final String quotedSourceName = UsageFormat.quoteValue(this.sourceName);
        final String valueString = this.getValueString();
        return String.format(this.format.toStringFormat, quotedSourceName, valueString);
    }
    
    public void calculateMaxLengths() {
        final long value = this.getUsageValue();
        this.format.calculateMaximumSourceNameLength(this.sourceName);
        this.format.calculateMaximumValueLength(value);
    }
    
    static {
        MAXIMUM_SMALL_VALUE = powerOfTwo(10) - 1L;
    }
}
