package com.datasphere.source.sm;

import java.util.*;

import com.datasphere.source.lib.prop.*;

public class SMProperty extends Property
{
    public final String ROW_DELIMITER = "rowdelimiter";
    public final String COLUMN_DELIMITER = "columndelimiter";
    public final String DEFAULT_ROW_DELIMITER = "\n";
    public final String DEFAULT_COLUMN_DELIMITER = ",";
    public final String DELIMITER_SPLIT_CHAR = "separator";
    public final String DEFAULT_DELIMITER_SPLIT_CHAR = ":";
    public final String QUOTECHAR = "quotecharacter";
    public final String QUOTE_SET = "quoteset";
    public final String ESCAPE_SEQUENCE = "escapesequence";
    public final String IGNORE_ROW_DELIMITER_IN_QUOTE = "IgnoreRowDelimiterInQuote";
    public final String DEFAULT_QUOTE_SET;
    public char commentcharacter;
    public String valueDelimiter;
    public String[] rowDelimiterList;
    public String[] columnDelimiterList;
    public String[] quoteSetList;
    public boolean escapeSequence;
    public boolean ignoreRowDelimiterInQueue;
    
    public SMProperty(final Map<String, Object> mp) {
        super(mp);
        this.DEFAULT_QUOTE_SET = Character.toString('\"');
        String delimiterSplitChar = (String)mp.get("separator");
        if (delimiterSplitChar == null) {
            this.getClass();
            delimiterSplitChar = ":";
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
        else {
            this.getClass();
            delimiter = "\n";
        }
        this.rowDelimiterList = delimiter.split(delimiterSplitChar);
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
        else {
            this.getClass();
            delimiter = ",";
        }
        if (mp.get("nocolumndelimiter") != null) {
            final boolean noColumnDelimiter = (boolean)mp.get("nocolumndelimiter");
            if (noColumnDelimiter) {
                delimiter = "";
            }
        }
        this.columnDelimiterList = delimiter.split(delimiterSplitChar);
        this.getClass();
        obj = mp.get("quoteset");
        if (obj == null) {
            this.getClass();
            obj = mp.get("quotecharacter");
        }
        if (obj != null) {
            if (obj instanceof String) {
                delimiter = (String)obj;
            }
            else {
                final char del = (char)obj;
                delimiter = Character.toString(del);
            }
        }
        else {
            delimiter = this.DEFAULT_QUOTE_SET;
        }
        this.quoteSetList = delimiter.split(delimiterSplitChar);
        this.getClass();
        obj = mp.get("escapesequence");
        if (obj != null) {
            this.escapeSequence = (boolean)obj;
        }
        else {
            this.escapeSequence = false;
        }
        this.getClass();
        obj = mp.get("IgnoreRowDelimiterInQuote");
        if (obj != null) {
            this.ignoreRowDelimiterInQueue = (boolean)obj;
        }
        else {
            this.ignoreRowDelimiterInQueue = false;
        }
    }
}
