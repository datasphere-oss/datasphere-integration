package com.datasphere.runtime;

import java.io.*;
import java.util.regex.*;

public class Interval implements Serializable
{
    private static final long serialVersionUID = -1040195849007951879L;
    private static Pattern numPat;
    private static Pattern yearmonthPat;
    private static Pattern daysecPat;
    private static Pattern dayhourPat;
    private static Pattern dayminPat;
    private static Pattern hourminPat;
    private static Pattern hoursecPat;
    private static Pattern minsecPat;
    private static Pattern secPat;
    private static final int secInMin = 60;
    private static final int secInHour = 3600;
    private static final int secInDay = 86400;
    public final long value;
    
    private static Pattern getYMpat(final int flags) {
        switch (flags) {
            case 16:
            case 32: {
                return Interval.numPat;
            }
            case 48: {
                return Interval.yearmonthPat;
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    private static Pattern getDSpat(final int flags) {
        switch (flags) {
            case 1: {
                return Interval.secPat;
            }
            case 2: {
                return Interval.numPat;
            }
            case 3: {
                return Interval.minsecPat;
            }
            case 4: {
                return Interval.numPat;
            }
            case 6: {
                return Interval.hourminPat;
            }
            case 7: {
                return Interval.hoursecPat;
            }
            case 8: {
                return Interval.numPat;
            }
            case 12: {
                return Interval.dayhourPat;
            }
            case 14: {
                return Interval.dayminPat;
            }
            case 15: {
                return Interval.daysecPat;
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    public static int parseMicrosecs(final String s) {
        if (s != null) {
            int val = Integer.parseInt(s);
            for (int len = s.length(); len < 6; ++len) {
                val *= 10;
            }
            return val;
        }
        return 0;
    }
    
    public Interval() {
        this.value = 0L;
    }
    
    public Interval(final long val) {
        this.value = val;
    }
    
    @Override
    public String toString() {
        long val = this.value;
        final long microsec = val % 1000000L;
        val /= 1000000L;
        return String.format("%d.%06d", val, microsec);
    }
    
    public static Interval parseDSInterval(final String literal, final int flags) {
        long days = 0L;
        long hours = 0L;
        long minutes = 0L;
        long seconds = 0L;
        long microsecs = 0L;
        final Matcher m = getDSpat(flags).matcher(literal);
        if (!m.matches()) {
            return null;
        }
        switch (flags) {
            case 1: {
                seconds = Integer.parseInt(m.group(1));
                microsecs = parseMicrosecs(m.group(3));
                break;
            }
            case 2: {
                minutes = Integer.parseInt(m.group(1));
                break;
            }
            case 3: {
                minutes = Integer.parseInt(m.group(1));
                seconds = Integer.parseInt(m.group(2));
                microsecs = parseMicrosecs(m.group(4));
                if (seconds > 59L) {
                    return null;
                }
                break;
            }
            case 4: {
                hours = Integer.parseInt(m.group(1));
                break;
            }
            case 6: {
                hours = Integer.parseInt(m.group(1));
                minutes = Integer.parseInt(m.group(2));
                if (minutes > 59L) {
                    return null;
                }
                break;
            }
            case 7: {
                hours = Integer.parseInt(m.group(1));
                minutes = Integer.parseInt(m.group(2));
                seconds = Integer.parseInt(m.group(3));
                microsecs = parseMicrosecs(m.group(5));
                if (minutes > 59L || seconds > 59L) {
                    return null;
                }
                break;
            }
            case 8: {
                days = Integer.parseInt(m.group(1));
                break;
            }
            case 12: {
                days = Integer.parseInt(m.group(1));
                hours = Integer.parseInt(m.group(2));
                if (hours > 23L) {
                    return null;
                }
                break;
            }
            case 14: {
                days = Integer.parseInt(m.group(1));
                hours = Integer.parseInt(m.group(2));
                minutes = Integer.parseInt(m.group(3));
                if (hours > 23L || minutes > 59L) {
                    return null;
                }
                break;
            }
            case 15: {
                days = Integer.parseInt(m.group(1));
                hours = Integer.parseInt(m.group(2));
                minutes = Integer.parseInt(m.group(3));
                seconds = Integer.parseInt(m.group(4));
                microsecs = parseMicrosecs(m.group(6));
                if (hours > 23L || minutes > 59L || seconds > 59L) {
                    return null;
                }
                break;
            }
            default: {
                assert false;
                break;
            }
        }
        final long secs = days * 86400L + hours * 3600L + minutes * 60L + seconds;
        return new Interval(secs * 1000000L + microsecs);
    }
    
    public static Interval parseYMInterval(final String literal, final int flags) {
        int months = 0;
        int years = 0;
        final Matcher m = getYMpat(flags).matcher(literal);
        if (!m.matches()) {
            return null;
        }
        switch (flags) {
            case 16: {
                months = Integer.parseInt(m.group(1));
                if (months < 1 || months > 12) {
                    return null;
                }
                break;
            }
            case 32: {
                years = Integer.parseInt(m.group(1));
                break;
            }
            case 48: {
                years = Integer.parseInt(m.group(1));
                months = Integer.parseInt(m.group(2));
                if (months < 1 || months > 12) {
                    return null;
                }
                break;
            }
            default: {
                assert false;
                break;
            }
        }
        return new Interval(years * 12 + months);
    }
    
    public String toHumanReadable() {
        final String sign = (this.value < 0L) ? "-" : "";
        final long seconds = Math.abs(this.value / 1000000L);
        final long microseconds = Math.abs(this.value % 1000000L);
        if (microseconds == 0L) {
            if (seconds >= 60L) {
                if (seconds % 86400L == 0L) {
                    return String.format("%s%d DAY", sign, seconds / 86400L);
                }
                if (seconds % 3600L == 0L) {
                    return String.format("%s%d HOUR", sign, seconds / 3600L);
                }
                if (seconds % 60L == 0L) {
                    return String.format("%s%d MINUTE", sign, seconds / 60L);
                }
            }
            return String.format("%s%d SECOND", sign, seconds);
        }
        return String.format("%s%d.%06d SECOND", sign, seconds, microseconds);
    }
    
    static {
        Interval.numPat = Pattern.compile("(\\d{1,9})");
        Interval.yearmonthPat = Pattern.compile("(\\d{1,9})-(\\d{1,2})");
        Interval.daysecPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2}):(\\d{1,2}):(\\d{1,2})(\\.(\\d{1,6}))?");
        Interval.dayhourPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2})");
        Interval.dayminPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2}):(\\d{1,2})");
        Interval.hourminPat = Pattern.compile("(\\d{1,9}):(\\d{1,2})");
        Interval.hoursecPat = Pattern.compile("(\\d{1,9}):(\\d{1,2}):(\\d{1,2})(\\.(\\d{1,6}))?");
        Interval.minsecPat = Pattern.compile("(\\d{1,9}):(\\d{1,2})(\\.(\\d{1,6}))?");
        Interval.secPat = Pattern.compile("(\\d{1,9})(\\.(\\d{1,6}))?");
    }
}
