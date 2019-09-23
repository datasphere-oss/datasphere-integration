package com.datasphere.runtime.compiler.exprs;

import java.sql.Timestamp;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import com.datasphere.runtime.Interval;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.visitors.ExpressionVisitor;

public class Constant extends ValueExpr
{
    private Class<?> valueType;
    public final Object value;
    private static DateTimeFormatter defaultDateTimeFormatter;
    
    public Constant(final ExprCmd kind, final Object val, final Class<?> type) {
        super(kind);
        this.value = val;
        this.valueType = type;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.value;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitConstant(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Constant)) {
            return false;
        }
        final Constant o = (Constant)other;
        return (this.value == null && o.value == null) || (super.equals(o) && this.value.equals(o.value));
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.value).append(super.hashCode()).toHashCode();
    }
    
    public static Constant newString(final String val) {
        return new Constant(ExprCmd.STRING, val, String.class);
    }
    
    public static Constant newInt(final int val) {
        return new Constant(ExprCmd.INT, val, Integer.TYPE);
    }
    
    public static Constant newLong(final Long val) {
        return new Constant(ExprCmd.LONG, val, Long.TYPE);
    }
    
    public static Constant newFloat(final Float val) {
        return new Constant(ExprCmd.FLOAT, val, Float.TYPE);
    }
    
    public static Constant newDouble(final Double val) {
        return new Constant(ExprCmd.DOUBLE, val, Double.TYPE);
    }
    
    public static Constant newBool(final Boolean val) {
        return new Constant(ExprCmd.BOOL, val, Boolean.TYPE);
    }
    
    public static Constant newNull() {
        return new Constant(ExprCmd.NULL, null, CompilerUtils.NullType.class);
    }
    
    public static Constant newDate(final String literal) {
        final Timestamp t = Timestamp.valueOf(literal);
        return new Constant(ExprCmd.TIMESTAMP, t, Timestamp.class);
    }
    
    private static DateTimeFormatter makeDefaultFormatter() {
        final DateTimeParser tp = ISODateTimeFormat.timeElementParser().getParser();
        final DateTimeParser otp = new DateTimeFormatterBuilder().appendLiteral(' ').appendOptional(tp).toParser();
        final DateTimeParser[] parsers = { new DateTimeFormatterBuilder().append(ISODateTimeFormat.dateElementParser().getParser()).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy/MM/dd")).appendOptional(otp).toParser(), ISODateTimeFormat.dateOptionalTimeParser().getParser(), tp };
        final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append((DateTimePrinter)null, parsers).toFormatter();
        return formatter;
    }
    
    public static DateTime parseDateTime(final String literal) {
        return DateTime.parse(literal, Constant.defaultDateTimeFormatter);
    }
    
    public static Constant newDateTime(final String literal) {
        final DateTime d = parseDateTime(literal);
        return new Constant(ExprCmd.DATETIME, d, DateTime.class);
    }
    
    public static Constant newYMInterval(final Interval value) {
        return new Constant(ExprCmd.INTERVAL, value.value, Long.TYPE);
    }
    
    public static Constant newDSInterval(final Interval value) {
        return new Constant(ExprCmd.INTERVAL, value.value, Long.TYPE);
    }
    
    public static Constant newClassRef(final Class<?> value) {
        return new Constant(ExprCmd.CLASS, value, Class.class);
    }
    
    @Override
    public Class<?> getType() {
        return this.valueType;
    }
    
    @Override
    public boolean isConst() {
        return true;
    }
    
    static {
        Constant.defaultDateTimeFormatter = makeDefaultFormatter();
    }
}
