package org.joda.time.base;

import java.io.*;
import org.joda.time.chrono.*;
import org.joda.time.*;
import org.joda.time.convert.*;

public abstract class BaseDateTime extends AbstractDateTime implements ReadableDateTime, Serializable
{
    private static final long serialVersionUID = -6728882245981L;
    private volatile long iMillis;
    private transient volatile Chronology iChronology;
    
    public BaseDateTime() {
        this(DateTimeUtils.currentTimeMillis(), (Chronology)ISOChronology.getInstance());
    }
    
    public BaseDateTime(final DateTimeZone zone) {
        this(DateTimeUtils.currentTimeMillis(), (Chronology)ISOChronology.getInstance(zone));
    }
    
    public BaseDateTime(final Chronology chronology) {
        this(DateTimeUtils.currentTimeMillis(), chronology);
    }
    
    public BaseDateTime(final long instant) {
        this(instant, (Chronology)ISOChronology.getInstance());
    }
    
    public BaseDateTime(final long instant, final DateTimeZone zone) {
        this(instant, (Chronology)ISOChronology.getInstance(zone));
    }
    
    public BaseDateTime(final long instant, final Chronology chronology) {
        this.iChronology = this.checkChronology(chronology);
        this.iMillis = this.checkInstant(instant, this.iChronology);
    }
    
    public BaseDateTime(final Object instant, final DateTimeZone zone) {
        final InstantConverter converter = ConverterManager.getInstance().getInstantConverter(instant);
        final Chronology chrono = this.checkChronology(converter.getChronology(instant, zone));
        this.iChronology = chrono;
        this.iMillis = this.checkInstant(converter.getInstantMillis(instant, chrono), chrono);
    }
    
    public BaseDateTime(final Object instant, final Chronology chronology) {
        final InstantConverter converter = ConverterManager.getInstance().getInstantConverter(instant);
        this.iChronology = this.checkChronology(converter.getChronology(instant, chronology));
        this.iMillis = this.checkInstant(converter.getInstantMillis(instant, chronology), this.iChronology);
    }
    
    public BaseDateTime(final int year, final int monthOfYear, final int dayOfMonth, final int hourOfDay, final int minuteOfHour, final int secondOfMinute, final int millisOfSecond) {
        this(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, (Chronology)ISOChronology.getInstance());
    }
    
    public BaseDateTime(final int year, final int monthOfYear, final int dayOfMonth, final int hourOfDay, final int minuteOfHour, final int secondOfMinute, final int millisOfSecond, final DateTimeZone zone) {
        this(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, (Chronology)ISOChronology.getInstance(zone));
    }
    
    public BaseDateTime(final int year, final int monthOfYear, final int dayOfMonth, final int hourOfDay, final int minuteOfHour, final int secondOfMinute, final int millisOfSecond, final Chronology chronology) {
        this.iChronology = this.checkChronology(chronology);
        final long instant = this.iChronology.getDateTimeMillis(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond);
        this.iMillis = this.checkInstant(instant, this.iChronology);
    }
    
    protected Chronology checkChronology(final Chronology chronology) {
        return DateTimeUtils.getChronology(chronology);
    }
    
    protected long checkInstant(final long instant, final Chronology chronology) {
        return instant;
    }
    
    public long getMillis() {
        return this.iMillis;
    }
    
    public Chronology getChronology() {
        return this.iChronology;
    }
    
    protected void setMillis(final long instant) {
        this.iMillis = this.checkInstant(instant, this.iChronology);
    }
    
    protected void setChronology(final Chronology chronology) {
        this.iChronology = this.checkChronology(chronology);
    }
}
