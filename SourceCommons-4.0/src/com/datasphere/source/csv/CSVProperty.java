package com.datasphere.source.csv;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

import java.util.*;

public class CSVProperty extends Property
{
    public final String TRIM_QUOTE = "trimquote";
    public final String IGNORE_EMPTY_COLUM = "IgnoreEmptyColumn";
    public final String IGNORE_EMPTY_COLUMN_CI = "ignoreemptycolumn";
    public boolean header;
    public char commentcharacter;
    public int columnDelimitTill;
    public boolean trimQuote;
    public boolean ignoreEmptyColums;
    Map<String, Object> map;
    
    @Override
    public Map<String, Object> getMap() {
        return this.map;
    }
    
    public CSVProperty(final Map<String, Object> mp) {
        super(mp);
        this.map = mp;
        if (mp.get("header") != null) {
            this.header = this.getBoolean(mp, "header");
            if (this.header) {
                this.headerlineno = Constant.HEADER_LINE_NO_DEFAULT;
            }
            else {
                this.headerlineno = 0;
                if (mp.get("headerlineno") != null) {
                    this.headerlineno = this.getInt(mp, "headerlineno");
                }
            }
        }
        else {
            this.header = false;
            this.headerlineno = 0;
            if (mp.get("headerlineno") != null) {
                this.headerlineno = this.getInt(mp, "headerlineno");
            }
        }
        if (mp.get("wildcard") == null) {
            this.wildcard = "*.csv";
        }
        this.commentcharacter = this.getChar(mp, "commentcharacter");
        if (mp.get("archivedir") != null) {
            this.archivedir = (String)mp.get("archivedir");
        }
        else {
            this.archivedir = null;
        }
        if (mp.get("columndelimittill") != null) {
            this.columnDelimitTill = this.getInt(mp, "columndelimittill");
            if (this.columnDelimitTill >= 0) {}
        }
        else {
            this.columnDelimitTill = -1;
        }
        if (mp.get("portno") != null) {
            this.portno = this.getInt(mp, "portno");
        }
        else {
            this.portno = 0;
        }
        this.getClass();
        if (mp.get("trimquote") != null) {
            this.getClass();
            this.trimQuote = this.getBoolean(mp, "trimquote");
        }
        else {
            this.trimQuote = true;
        }
        this.getClass();
        if (mp.get("IgnoreEmptyColumn") == null) {
            this.getClass();
            if (mp.get("ignoreemptycolumn") == null) {
                this.ignoreEmptyColums = false;
                return;
            }
        }
        this.getClass();
        this.ignoreEmptyColums = this.getBoolean(mp, "ignoreemptycolumn");
    }
}
