package com.datasphere.source.smlite;

import java.util.*;

import com.datasphere.source.lib.prop.*;

public class SMProperty extends Property
{
    public char commentcharacter;
    public String[] rowDelimiterList;
    public String[] columnDelimiterList;
    public String[] quoteSetList;
    public String[] escapeList;
    public String[] pairDelimiterList;
    public boolean escapeSequence;
    public boolean ignoreRowDelimiterInQuote;
    public boolean blockAsCompleteRecord;
    public String[] timeStamp;
    public String[] recordBegin;
    public String[] recordEnd;
    public String delimiterSplitChar;
    public boolean trimQuote;
    public boolean recordBeginWithTS;
    
    public SMProperty(final Map<String, Object> mp) {
        super(mp);
        this.columnDelimiterList = new String[0];
        this.delimiterSplitChar = (String)mp.get("separator");
        if (this.delimiterSplitChar == null) {
            this.delimiterSplitChar = ":";
        }
        this.commentcharacter = this.getChar(mp, "commentcharacter");
        String delimiter = "";
        Object obj = mp.get("rowdelimiter");
        if (obj != null) {
            if (obj instanceof String) {
                delimiter = (String)obj;
            }
            else {
                final char del = (char)mp.get("rowdelimiter");
                delimiter = Character.toString(del);
            }
        }
        this.rowDelimiterList = delimiter.split(this.delimiterSplitChar);
        obj = mp.get("columndelimiter");
        if (obj != null) {
            if (obj instanceof String) {
                delimiter = (String)obj;
            }
            else {
                final char del = (char)mp.get("columndelimiter");
                delimiter = Character.toString(del);
            }
        }
        boolean noColumnDelimiter = false;
        if (mp.get("nocolumndelimiter") != null) {
            noColumnDelimiter = (boolean)mp.get("nocolumndelimiter");
        }
        if (!noColumnDelimiter) {
            this.columnDelimiterList = delimiter.split(this.delimiterSplitChar);
        }
        obj = mp.get("pairdelimiter");
        if (obj != null) {
            delimiter = (String)obj;
            this.pairDelimiterList = delimiter.split(this.delimiterSplitChar);
        }
        else {
            this.pairDelimiterList = null;
        }
        obj = mp.get("quoteset");
        if (obj == null) {
            obj = mp.get("quotecharacter");
        }
        if (obj != null) {
            if (obj instanceof String) {
                delimiter = (String)obj;
            }
            else {
                final char del2 = (char)obj;
                delimiter = Character.toString(del2);
            }
            this.quoteSetList = delimiter.split(this.delimiterSplitChar);
        }
        obj = mp.get("escapesequence");
        if (obj != null) {
            this.escapeSequence = (boolean)obj;
        }
        else {
            this.escapeSequence = false;
        }
        obj = mp.get("IgnoreRowDelimiterInQuote");
        if (obj != null) {
            this.ignoreRowDelimiterInQuote = (boolean)obj;
        }
        else {
            this.ignoreRowDelimiterInQuote = false;
        }
        this.blockAsCompleteRecord = this.getBoolean("blockAsCompleteRecord", false);
        String tmp = this.getString("TimeStamp", "");
        if (!tmp.isEmpty()) {
            this.timeStamp = tmp.split(this.delimiterSplitChar);
        }
        tmp = this.getString("RecordBegin", "");
        if (!tmp.isEmpty()) {
            this.recordBegin = tmp.split(this.delimiterSplitChar);
        }
        delimiter = this.getString("escapecharacter", "");
        if (!delimiter.isEmpty()) {
            this.escapeList = delimiter.split(this.delimiterSplitChar);
        }
        this.trimQuote = this.getBoolean("trimquote", true);
        tmp = this.getString("RecordEnd", "");
        if (!tmp.isEmpty()) {
            this.recordEnd = tmp.split(this.delimiterSplitChar);
        }
        this.recordBeginWithTS = this.getBoolean("RecordBeginWithTimeStamp", false);
    }
}
