package com.datasphere.source.smlite;

import org.apache.log4j.*;
import com.datasphere.common.exc.*;
import java.util.*;

public abstract class CharParser
{
    Logger logger;
    public static final int TABLE_SIZE = 65536;
    protected StateMachine stateMachine;
    SMProperty prop;
    String[] colDelimiter;
    String[] rowDelimiter;
    String[] quote;
    String[] nvpDelimiter;
    String[][] delimiterList;
    boolean hasRecordBeginSupport;
    Map<String, SMEvent> delimiterEventMap;
    Map<String, String> relatedDelimiters;
    private int maxDelimiterSize;
    protected String matchedDelimiter;
    
    public CharParser(final StateMachine sm, final SMProperty prop) {
        this.logger = Logger.getLogger((Class)CharParser.class);
        this.stateMachine = sm;
        this.prop = prop;
    }
    
    protected void init() {
        this.colDelimiter = this.prop.columnDelimiterList;
        this.rowDelimiter = this.prop.rowDelimiterList;
        this.quote = this.prop.quoteSetList;
        this.nvpDelimiter = this.prop.pairDelimiterList;
        this.delimiterList = new String[13][];
        this.delimiterEventMap = new HashMap<String, SMEvent>();
        this.relatedDelimiters = new HashMap<String, String>();
        this.initializeDelimiters();
    }
    
    protected void initializeDelimiters() {
        this.initializeDelimiter(this.prop.timeStamp, (short)9);
        this.initializeDelimiter(this.prop.columnDelimiterList, (short)1);
        this.initializeDelimiter(this.prop.rowDelimiterList, (short)2);
        if (this.prop.recordBeginWithTS) {
            this.initializeDelimiter(this.prop.timeStamp, (short)8);
        }
        this.initializeDelimiter(this.prop.recordBegin, (short)8);
        if (this.prop.recordBegin != null && this.prop.recordBegin.length > 0) {
            this.hasRecordBeginSupport = true;
        }
        this.initializeDelimiter(this.prop.recordEnd, (short)11);
        this.initializeDelimiter(this.prop.escapeList, (short)10);
        if (this.prop.commentcharacter != '\0') {
            this.initializeDelimiter(this.prop.commentcharacter, (short)6);
        }
        this.initializeDelimiter(this.prop.pairDelimiterList, (short)12);
        if (this.prop.blockAsCompleteRecord) {
            this.initializeDelimiter('\0', (short)7);
        }
        this.initializeQuoteSet(this.prop.quoteSetList);
        this.finalizeDelimiterInit();
    }
    
    private void initializeDelimiter(final char c, final short eventType) {
        final String[] tmp = { "" + c };
        this.initializeDelimiter(tmp, eventType);
    }
    
    protected void initializeDelimiter(final String[] delimiter, final short event) {
        for (int itr = 0; itr < delimiter.length; ++itr) {
            if (this.maxDelimiterSize < delimiter[itr].length()) {
                this.maxDelimiterSize = delimiter[itr].length();
            }
        }
        this.delimiterList[event] = delimiter;
    }
    
    private void initializeQuoteSet(final String[] quoteSet) {
        if (quoteSet == null) {
            return;
        }
        final List<String> quote = new ArrayList<String>();
        final List<String> qBegin = new ArrayList<String>();
        final List<String> qEnd = new ArrayList<String>();
        for (int itr = 0; itr < quoteSet.length && quoteSet[itr].length() > 0; ++itr) {
            final int len = quoteSet[itr].length();
            if (len == 1) {
                quote.add("" + quoteSet[itr].charAt(0));
            }
            else {
                final String begin = quoteSet[itr].substring(0, len / 2);
                final String end = quoteSet[itr].substring(len / 2, len);
                if (len % 2 != 0) {
                    this.logger.warn((Object)("Non even lengh quote-set is specfied. QuoteSet {" + quoteSet[itr] + "} will be split BeginQuote as {" + begin + "} and EndQuote as {" + end + "}"));
                }
                qBegin.add(begin);
                qEnd.add(end);
                this.relatedDelimiters.put(begin, end);
            }
        }
        if (quote.size() > 0) {
            this.initializeDelimiter(quote.toArray(new String[quote.size()]), (short)3);
        }
        if (qBegin.size() > 0) {
            this.initializeDelimiter(qBegin.toArray(new String[qBegin.size()]), (short)4);
            this.initializeDelimiter(qEnd.toArray(new String[qEnd.size()]), (short)5);
        }
    }
    
    public int addIntoLookupTable(final char c) {
        return 0;
    }
    
    public int addIntoLookupTable(final char c, final int ordinal) {
        return ordinal;
    }
    
    public int addIntoGroupTable(final String str) {
        return 0;
    }
    
    public int addIntoGroupTable(final int groupId, final char c) {
        return 0;
    }
    
    public int computeHashFor(final String str) {
        return 0;
    }
    
    public abstract void ignoreEvents(final short[] p0);
    
    public abstract void ignoreEvents(final short[] p0, final SMEvent p1);
    
    public abstract void next() throws RecordException, AdapterException;
    
    public abstract boolean hasNext();
    
    public void validateProperty() throws AdapterException {
    }
    
    public void reset() {
    }
    
    public void setEventForGroup(final int gId, final char c, final SMEvent event) {
    }
    
    public boolean hasRecordBeginSupport() {
        return this.hasRecordBeginSupport;
    }
    
    public int maxDelimiterLength() {
        return this.maxDelimiterSize;
    }
    
    public void finalizeDelimiterInit() {
        for (final String entry : this.relatedDelimiters.keySet()) {
            final String relatedDel = this.relatedDelimiters.get(entry);
            final SMEvent event = this.delimiterEventMap.get(entry);
            final SMEvent relatedEvent = this.delimiterEventMap.get(relatedDel);
            event.relatedEvent = relatedEvent;
        }
    }
    
    public void close() {
    }
}
