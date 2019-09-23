package com.datasphere.runtime.utils;

import org.joda.time.*;
import java.util.*;
import org.joda.time.format.*;

public class DateParser
{
    PartDesc[] parts;
    private String lastDate;
    private DateTime lastDateTime;
    
    public DateParser(final String format) {
        this.lastDate = null;
        this.lastDateTime = null;
        final char[] formatChars = format.toCharArray();
        final List<PartDesc> partList = new ArrayList<PartDesc>();
        for (final char c : formatChars) {
            PartDesc currPart = null;
            if (partList.size() > 0) {
                currPart = partList.get(partList.size() - 1);
            }
            PartType type = PartType.unknown;
            switch (c) {
                case 'y': {
                    type = PartType.year;
                    break;
                }
                case 'M': {
                    type = PartType.month;
                    break;
                }
                case 'd': {
                    type = PartType.day;
                    break;
                }
                case 'H': {
                    type = PartType.hour;
                    break;
                }
                case 'm': {
                    type = PartType.minute;
                    break;
                }
                case 's': {
                    type = PartType.second;
                    break;
                }
                case 'S': {
                    type = PartType.milli;
                    break;
                }
                case 'z': {
                    type = PartType.zone1;
                    break;
                }
                case 'Z': {
                    type = PartType.zone2;
                    break;
                }
                case ' ':
                case '-':
                case '.':
                case '/':
                case ':':
                case 'T':
                case '_': {
                    type = PartType.punc;
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Date format character '" + c + "' in " + format + " is not permitted");
                }
            }
            if (currPart == null || !type.equals(currPart.type)) {
                currPart = new PartDesc();
                currPart.type = type;
                partList.add(currPart);
            }
            final PartDesc partDesc = currPart;
            ++partDesc.length;
            final StringBuilder sb = new StringBuilder();
            final PartDesc partDesc2 = currPart;
            partDesc2.chars = sb.append(partDesc2.chars).append(c).toString();
        }
        this.parts = new PartDesc[partList.size()];
        this.parts = partList.toArray(this.parts);
    }
    
    public DateTime parse(final String date) {
        synchronized (this.parts) {
            if (date.equals(this.lastDate)) {
                return this.lastDateTime;
            }
        }
        int y = 0;
        int M = 0;
        int d = 0;
        int h = 0;
        int m = 0;
        int s = 0;
        int S = 0;
        final char[] chars = date.toCharArray();
        int offset = 0;
        int partIndx = 0;
        PartDesc currPart = this.parts[partIndx];
        try {
            while (offset < chars.length) {
                switch (currPart.type) {
                    case day: {
                        d = currPart.parse(chars, offset);
                        break;
                    }
                    case hour: {
                        h = currPart.parse(chars, offset);
                        break;
                    }
                    case milli: {
                        S = currPart.parse(chars, offset);
                        break;
                    }
                    case minute: {
                        m = currPart.parse(chars, offset);
                        break;
                    }
                    case month: {
                        M = currPart.parse(chars, offset);
                    }
                    case second: {
                        s = currPart.parse(chars, offset);
                        break;
                    }
                    case year: {
                        y = currPart.parse(chars, offset);
                        break;
                    }
                }
                offset += currPart.length;
                if (++partIndx < this.parts.length) {
                    currPart = this.parts[partIndx];
                }
            }
        }
        catch (Throwable t) {
            throw new RuntimeException("Problem parsing date " + date + " using format " + this.toString(), t);
        }
        final DateTime ret = new DateTime(y, M, d, h, m, s, S);
        synchronized (this.parts) {
            this.lastDate = date;
            this.lastDateTime = ret;
        }
        return ret;
    }
    
    @Override
    public String toString() {
        String ret = "";
        for (final PartDesc part : this.parts) {
            ret += part.chars;
        }
        return ret;
    }
    
    public static void main(final String[] args) {
        final DateParser dps = new DateParser("yyyy-MMM-ddTHH:mm:ss.SSS Z");
        final String[] array;
        final String[] months = array = new String[] { "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" };
        for (final String m : array) {
            String date = "2015-" + m + "-27T18:01:35.000 Z";
            DateTime ds = dps.parse(date);
            System.out.println("Date parser " + dps + " for " + date + " returns " + ds);
            date = "2015-" + m.toUpperCase() + "-27T18:01:35.000 Z";
            ds = dps.parse(date);
            System.out.println("Date parser " + dps + " for " + date + " returns " + ds);
        }
        final DateParser dp = new DateParser("yyyy/MM/dd HH:mm:ss.SSS");
        DateTime d = dp.parse("2013/11/05 16:16:16.160");
        System.out.println("Date parser " + dp + " returns " + d);
        final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
        d = dtf.parseDateTime("2013/11/05 16:16:16.160");
        System.out.println("DateTimeFormat returns " + d);
        final long numRuns = 10000000L;
        System.out.println("Warming DateTimeFormat");
        for (int i = 0; i < numRuns; ++i) {
            d = dtf.parseDateTime("2013/11/05 16:16:16.160");
        }
        System.out.println("Warming DateParser");
        for (int i = 0; i < numRuns; ++i) {
            d = dp.parse("2013/11/05 16:16:16.160");
        }
        long stime = System.currentTimeMillis();
        System.out.println("Running DateTimeFormat");
        for (int j = 0; j < numRuns; ++j) {
            d = dtf.parseDateTime("2013/11/05 16:16:16.160");
        }
        long etime = System.currentTimeMillis();
        long diff = etime - stime;
        long rate = numRuns * 1000L / diff;
        System.out.println("Completed DateTimeFormat in " + diff + " ms rate " + rate + " per/s");
        stime = System.currentTimeMillis();
        System.out.println("Running DateParser");
        for (int j = 0; j < numRuns; ++j) {
            d = dp.parse("2013/11/05 16:16:16.160");
        }
        etime = System.currentTimeMillis();
        diff = etime - stime;
        rate = numRuns * 1000L / diff;
        System.out.println("Completed DateParser in " + diff + " ms rate " + rate + " per/s");
    }
    
    enum PartType
    {
        unknown, 
        punc, 
        year, 
        month, 
        day, 
        hour, 
        minute, 
        second, 
        milli, 
        zone1, 
        zone2;
    }
    
    class PartDesc
    {
        PartType type;
        int length;
        String chars;
        
        PartDesc() {
            this.length = 0;
            this.chars = "";
        }
        
        public int parseInt(final char[] chars, final int offset, final int len) {
            int ret = 0;
            for (int i = offset; i < offset + len; ++i) {
                ret *= 10;
                final char c = chars[i];
                final int val = c - '0';
                if (val < 0 || val > 9) {
                    throw new RuntimeException("Illegal value parsing " + this.type + " '" + c + "' in " + new String(chars, offset, len));
                }
                ret += val;
            }
            return ret;
        }
        
        public int parse(final char[] chars, final int offset) {
            final int ret = 0;
            switch (this.type) {
                case day: {
                    if (this.length == 2 || this.length == 1) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    break;
                }
                case hour: {
                    if (this.length == 2 || this.length == 1) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    break;
                }
                case milli: {
                    if (this.length > 0 && this.length < 4) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    break;
                }
                case minute: {
                    if (this.length == 2 || this.length == 1) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    break;
                }
                case month: {
                    if (this.length == 2 || this.length == 1) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    if (this.length == 3) {
                        if (chars[offset] == 'A' || chars[offset] == 'a') {
                            if (chars[offset + 1] == 'P' || chars[offset + 1] == 'p') {
                                return 4;
                            }
                            if (chars[offset + 1] == 'U' || chars[offset + 1] == 'u') {
                                return 8;
                            }
                        }
                        else {
                            if (chars[offset] == 'D' || chars[offset] == 'd') {
                                return 12;
                            }
                            if (chars[offset] == 'F' || chars[offset] == 'f') {
                                return 2;
                            }
                            if (chars[offset] == 'J' || chars[offset] == 'j') {
                                if (chars[offset + 1] == 'A' || chars[offset + 1] == 'a') {
                                    return 1;
                                }
                                if (chars[offset + 1] == 'U' || chars[offset + 1] == 'u') {
                                    if (chars[offset + 2] == 'N' || chars[offset + 2] == 'n') {
                                        return 6;
                                    }
                                    if (chars[offset + 2] == 'L' || chars[offset + 2] == 'l') {
                                        return 7;
                                    }
                                }
                            }
                            else if (chars[offset] == 'M' || chars[offset] == 'm') {
                                if (chars[offset + 2] == 'R' || chars[offset + 2] == 'r') {
                                    return 3;
                                }
                                if (chars[offset + 2] == 'Y' || chars[offset + 2] == 'y') {
                                    return 5;
                                }
                            }
                            else {
                                if (chars[offset] == 'N' || chars[offset] == 'n') {
                                    return 11;
                                }
                                if (chars[offset] == 'O' || chars[offset] == 'o') {
                                    return 10;
                                }
                                if (chars[offset] == 'S' || chars[offset] == 's') {
                                    return 9;
                                }
                            }
                        }
                    }
                    throw new IllegalArgumentException("Invalid month value in " + new String(chars));
                }
                case second: {
                    if (this.length == 2 || this.length == 1) {
                        return this.parseInt(chars, offset, this.length);
                    }
                    break;
                }
                case year: {
                    if (this.length > 0 && this.length < 5) {
                        int y = this.parseInt(chars, offset, this.length);
                        if (this.length <= 2) {
                            y = ((y < 33) ? (2000 + y) : (1900 + y));
                        }
                        return y;
                    }
                    break;
                }
                case punc: {}
                case unknown: {}
                case zone1: {}
            }
            return ret;
        }
    }
}
