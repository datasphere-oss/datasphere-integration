package com.datasphere.usagemetrics.tungsten;

class UsageFormat
{
    public static final String SMALL_VALUE_FORMAT = "%4d  %s";
    public static final String SCALED_VALUE_FORMAT = "%6.1f%s%s";
    private static final String FULL_FORMAT = "Data Source %%-%ds : %%s";
    private static final String QUOTING_FORMAT = "'%s'";
    private static final String GROUPED_FORMAT = "%,d";
    private static final String BYTE_COUNT_FORMAT = " (%%,%dd bytes)";
    private int sourceNameMaxLength;
    private int valueMaxLength;
    public String toStringFormat;
    public String valueFormat;
    
    UsageFormat() {
        this.sourceNameMaxLength = 0;
        this.valueMaxLength = 0;
        this.toStringFormat = "";
        this.valueFormat = "";
    }
    
    public void calculateMaximumSourceNameLength(final String sourceName) {
        this.sourceNameMaxLength = calculateMaximumLength(this.sourceNameMaxLength, sourceName);
        this.toStringFormat = String.format("Data Source %%-%ds : %%s", this.sourceNameMaxLength + 2);
    }
    
    public void calculateMaximumValueLength(final long value) {
        final String valueString = String.format("%,d", value);
        this.valueMaxLength = calculateMaximumLength(this.valueMaxLength, valueString);
        this.valueFormat = String.format(" (%%,%dd bytes)", this.valueMaxLength);
    }
    
    private static int calculateMaximumLength(final int currentMaximum, final String value) {
        return Math.max(currentMaximum, value.length());
    }
    
    public static String quoteValue(final String value) {
        return String.format("'%s'", value);
    }
    
    public static String getUnitName(final int scale) {
        switch (scale) {
            case 50: {
                return " PiB";
            }
            case 40: {
                return " TiB";
            }
            case 30: {
                return " GiB";
            }
            case 20: {
                return " MiB";
            }
            case 10: {
                return " KiB";
            }
            default: {
                return " bytes";
            }
        }
    }
}
