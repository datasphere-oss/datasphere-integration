package com.datasphere.source.csv;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.source.lib.intf.SMCallback;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.reader.ReaderBase;
import com.datasphere.source.lib.rs.CharResultSet;
import com.datasphere.source.lib.rs.MetaData;
import com.datasphere.source.lib.utils.ReaderMetadataPersistenceUtility;
import com.datasphere.source.nvp.NVPProperty;
import com.datasphere.source.nvp.NameValueParser;
import com.datasphere.source.smlite.ColumnEvent;
import com.datasphere.source.smlite.CommentEvent;
import com.datasphere.source.smlite.EndOfBlockEvent;
import com.datasphere.source.smlite.EscapeEvent;
import com.datasphere.source.smlite.EventFactory;
import com.datasphere.source.smlite.QuoteBeginEvent;
import com.datasphere.source.smlite.QuoteEndEvent;
import com.datasphere.source.smlite.QuoteEvent;
import com.datasphere.source.smlite.ResetEvent;
import com.datasphere.source.smlite.RowBeginEvent;
import com.datasphere.source.smlite.RowEndEvent;
import com.datasphere.source.smlite.RowEvent;
import com.datasphere.source.smlite.SMEvent;
import com.datasphere.source.smlite.SMProperty;
import com.datasphere.source.smlite.StateMachine;
import com.datasphere.source.smlite.StateMachineBuilder;
import com.datasphere.source.smlite.TimeStampEvent;

public class CSVResultSetLite extends CharResultSet implements SMCallback
{
    Logger logger;
    StateMachine sm;
    Reader dataSource;
    int colCount;
    boolean gotCompleteRecord;
    boolean inQuote;
    SMProperty smProp;
    CSVProperty csvProp;
    short[] quoteSetEventIgnoreList;
    short[] quoteEventIgnoreList;
    short[] commentEventIgnoreList;
    short[] columnDelimitTillIgnoreList;
    short[] recordBeginIgnoreEventList;
    CSVResultSetMetaData resultSetMetadata;
    char[] recordBuffer;
    boolean hasGotRecordEnd;
    boolean ignoreMultipleRecordBegin;
    String timeStamp;
    int beginOfTimeStamp;
    int lengthOfTimeStamp;
    String dateString;
    boolean hasGotTimestamp;
    boolean seenRecordBegin;
    int lineSkipCount;
    RowEvent rEvent;
    RowEvent dummyRowEvent;
    boolean hasValidRecord;
    boolean inEscape;
    int escapeOffset;
    boolean seenQuote;
    boolean inComment;
    boolean delimittedColumn;
    boolean rollOverFlag;
    long recordBeginOffset;
    long logicalRecordBeginOffset;
    long trashedDataLen;
    ArrayList<Integer> quoteBeginPosition;
    ArrayList<Integer> quoteEndPosition;
    ArrayList<Integer> quoteLength;
    String quoteToMatch;
    Stack<SMEvent> quoteStack;
    String dateFormat;
    Date eventTime;
    Map<String, SimpleDateFormat> dateFormaterMap;
    private boolean firstEvent;
    private long eventTimestamp;
    long chkpntBeginOff;
    long recLength;
    long lastRecordMark;
    int pos;
    int lastPos;
    long lastRecordBeginPosition;
    long unwantedDataLen;
    
    public CSVResultSetLite(final Reader reader, final Property prop) throws IOException, InterruptedException, AdapterException {
        super(reader, prop);
        this.logger = Logger.getLogger((Class)CSVResultSetLite.class);
        this.hasGotRecordEnd = false;
        this.ignoreMultipleRecordBegin = false;
        this.beginOfTimeStamp = -1;
        this.recordBeginOffset = 0L;
        this.logicalRecordBeginOffset = 0L;
        this.trashedDataLen = 0L;
        this.firstEvent = false;
        this.eventTimestamp = 0L;
        this.csvProp = new CSVProperty(prop.getMap());
        this.dataSource = reader;
        this.smProp = new SMProperty(prop.getMap());
        this.init();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("CSV/DSVParser is initialized with following properties\nHeader - [" + this.csvProp.header + "]\nRowDelimiter - [" + Arrays.toString(this.smProp.rowDelimiterList) + "]\nColumnDelimiter - [" + Arrays.toString(this.smProp.columnDelimiterList) + "]\nTrimQuote - [" + this.smProp.trimQuote + "]\nQuoteSet - [" + Arrays.toString(this.smProp.quoteSetList) + "]\nColumnDelimitTill - [" + this.csvProp.columnDelimitTill + "]\nIgnoreEmptyColumn - [" + this.csvProp.ignoreEmptyColums + "]\nCommentCharacter - [" + this.smProp.commentcharacter + "]\nIgnoreRowDelimiterInQuote - [" + this.smProp.ignoreRowDelimiterInQuote + "]\nHeaderLineNumber - [" + this.csvProp.headerlineno + "]\nNoColumnDelimiter - [" + this.csvProp.nocolumndelimiter + "]\nSeparator - [" + prop.getMap().get(":") + "]\n" + "LineNumber - [" + this.csvProp.lineoffset + "]\nTrimWhiteSpace - [" + this.csvProp.trimwhitespace + "]"));
        }
    }
    
    private void init() throws IOException, InterruptedException, AdapterException {
        super.Init();
        final StateMachineBuilder builder = new StateMachineBuilder(this.smProp);
        this.ignoreMultipleRecordBegin = this.smProp.getBoolean("ignoremultiplerecordbegin", true);
        if (this.ignoreMultipleRecordBegin && this.smProp.recordEnd != null && this.smProp.recordEnd.length > 0) {
            this.hasGotRecordEnd = true;
        }
        (this.sm = builder.createStateMachine(this.dataSource, this.smProp)).callback(this);
        (this.dummyRowEvent = (RowEvent)EventFactory.createEvent((short)2)).array(this.sm.buffer());
        if (this.smProp.ignoreRowDelimiterInQuote) {
            (this.quoteEventIgnoreList = new short[8])[0] = 1;
            this.quoteEventIgnoreList[1] = 2;
            this.quoteEventIgnoreList[2] = 8;
            this.quoteEventIgnoreList[3] = 10;
            this.quoteEventIgnoreList[4] = 4;
            this.quoteEventIgnoreList[5] = 5;
            this.quoteEventIgnoreList[6] = 6;
            this.quoteEventIgnoreList[7] = 12;
            (this.quoteSetEventIgnoreList = new short[8])[0] = 1;
            this.quoteSetEventIgnoreList[1] = 2;
            this.quoteSetEventIgnoreList[2] = 8;
            this.quoteSetEventIgnoreList[4] = 3;
            this.quoteSetEventIgnoreList[5] = 5;
            this.quoteSetEventIgnoreList[6] = 6;
            this.quoteSetEventIgnoreList[7] = 12;
        }
        else {
            (this.quoteEventIgnoreList = new short[7])[0] = 1;
            this.quoteEventIgnoreList[1] = 8;
            this.quoteEventIgnoreList[2] = 10;
            this.quoteEventIgnoreList[3] = 4;
            this.quoteEventIgnoreList[4] = 5;
            this.quoteEventIgnoreList[5] = 6;
            this.quoteEventIgnoreList[6] = 12;
            (this.quoteSetEventIgnoreList = new short[7])[0] = 1;
            this.quoteSetEventIgnoreList[1] = 8;
            this.quoteSetEventIgnoreList[3] = 3;
            this.quoteSetEventIgnoreList[4] = 5;
            this.quoteSetEventIgnoreList[5] = 6;
            this.quoteSetEventIgnoreList[6] = 12;
        }
        (this.commentEventIgnoreList = new short[3])[0] = 1;
        this.commentEventIgnoreList[1] = 6;
        this.commentEventIgnoreList[2] = 10;
        (this.columnDelimitTillIgnoreList = new short[2])[0] = 1;
        this.columnDelimitTillIgnoreList[1] = 6;
        this.recordBeginIgnoreEventList = new short[] { 8 };
        if (this.sourceCheckpoint == null) {
            this.sourceCheckpoint = this.dataSource.getCheckpointDetail();
        }
        this.recordCheckpoint = this.sourceCheckpoint;
        this.reader().registerObserver(this);
        this.dateFormaterMap = new HashMap<String, SimpleDateFormat>();
        this.quoteBeginPosition = new ArrayList<Integer>();
        this.quoteEndPosition = new ArrayList<Integer>();
        this.quoteLength = new ArrayList<Integer>();
        this.quoteStack = new Stack<SMEvent>();
        if (this.smProp.getBoolean("header", false) && !this.recordCheckpoint.isRecovery()) {
            this.recordCount = -1;
            this.resultSetMetadata = new CSVResultSetMetaData(this, new CSVProperty(this.prop.propMap));
        }
        else {
            this.recordBeginOffset = this.recordCheckpoint.getRecordEndOffset();
        }
    }
    
    private void resetState() {
        this.lastRecordMark = 0L;
        this.logicalRecordBeginOffset = 0L;
        this.columnBeginOffset[0] = 0;
        this.colCount = 0;
        this.inQuote = false;
        this.inComment = false;
        this.dateFormat = null;
        this.dateString = null;
        this.beginOfTimeStamp = -1;
        this.lengthOfTimeStamp = -1;
        this.seenRecordBegin = false;
        this.hasGotTimestamp = false;
        this.delimittedColumn = false;
        this.sm.ignoreEvents(null);
        this.quoteStack.clear();
        this.quoteEndPosition.clear();
        this.quoteBeginPosition.clear();
        this.quoteLength.clear();
    }
    
    @Override
    public void onEvent(final EscapeEvent event) {
        if (this.inEscape) {
            this.inEscape = false;
            this.escapeOffset = -1;
        }
        else {
            this.inEscape = true;
            this.escapeOffset = event.position();
        }
    }
    
    @Override
    public void onEvent(final ColumnEvent cEvent) {
        if (this.inEscape && this.escapeEvent(cEvent)) {
            return;
        }
        this.setTimeStamp(cEvent);
        if (!this.inQuote) {
            this.setColumnData(cEvent);
            if (this.csvProp.columnDelimitTill != -1 && this.colCount >= this.csvProp.columnDelimitTill) {
                this.sm.ignoreEvents(this.columnDelimitTillIgnoreList);
                this.delimittedColumn = true;
            }
        }
        else if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Invalid column event detected while in Quote");
        }
    }
    
    @Override
    public void onEvent(final CommentEvent event) {
        if (this.logicalRecordBeginOffset + 1L == event.position()) {
            this.sm.ignoreEvents(this.commentEventIgnoreList);
            this.inComment = true;
        }
    }
    
    @Override
    public void onEvent(final RowEvent rowEvent) {
        if (this.inComment || (this.inEscape && this.escapeEvent(rowEvent)) || (this.prop.lineoffset > 0 && this.lineSkipCount < this.prop.lineoffset)) {
            if (this.inComment) {
                this.sm.ignoreEvents(null);
                this.inComment = false;
            }
            else if (this.prop.lineoffset > 0) {
                if (this.lineSkipCount + 1 == this.prop.lineoffset) {
                    this.sm.ignoreEvents(null);
                    if (this.delimittedColumn) {
                        this.delimittedColumn = false;
                    }
                }
                ++this.lineSkipCount;
            }
            this.colCount = 0;
            this.logicalRecordBeginOffset = rowEvent.position();
            this.columnBeginOffset[0] = (int)this.logicalRecordBeginOffset;
            this.sm.lastRecordEndPosition(rowEvent.currentPosition - rowEvent.length);
            return;
        }
        if (this.delimittedColumn) {
            this.sm.ignoreEvents(null);
            this.delimittedColumn = false;
        }
        if (rowEvent.position() == rowEvent.rowBegin()) {
            return;
        }
        if (this.colCount == 0 && rowEvent.position() - rowEvent.length() == rowEvent.rowBegin()) {
            this.logicalRecordBeginOffset = rowEvent.position();
            this.columnBeginOffset[0] = (int)this.logicalRecordBeginOffset;
            return;
        }
        this.seenRecordBegin = false;
        this.setRecordEnd(this.rEvent = rowEvent);
    }
    
    protected void setColumnData(final SMEvent event) {
        int offsetAdj = 0;
        if (event.removePattern) {
            this.columnOffset[this.colCount] = event.currentPosition - event.length;
        }
        else {
            this.columnOffset[this.colCount] = event.currentPosition;
        }
        if (this.seenQuote && this.csvProp.trimQuote) {
            for (int colBegin = this.columnBeginOffset[this.colCount], beginOffset = 0, itr = this.quoteBeginPosition.size() - 1; itr >= 0 && this.quoteEndPosition.get(itr) == this.columnOffset[this.colCount] && colBegin + offsetAdj + this.quoteLength.get(beginOffset) == this.quoteBeginPosition.get(beginOffset); offsetAdj += this.quoteLength.get(beginOffset), --itr, ++beginOffset) {
                final int[] columnBeginOffset = this.columnBeginOffset;
                final int colCount = this.colCount;
                columnBeginOffset[colCount] += this.quoteLength.get(itr);
                final int[] columnOffset = this.columnOffset;
                final int colCount2 = this.colCount;
                columnOffset[colCount2] -= this.quoteLength.get(itr);
            }
            this.seenQuote = false;
        }
        if (!this.csvProp.ignoreEmptyColums || this.columnOffset[this.colCount] - this.columnBeginOffset[this.colCount] > 0) {
            this.columnBeginOffset[this.colCount + 1] = this.columnOffset[this.colCount] + event.length + offsetAdj;
            offsetAdj = 0;
            ++this.colCount;
        }
        else {
            this.columnBeginOffset[this.colCount] = event.position();
        }
        this.quoteEndPosition.clear();
        this.quoteBeginPosition.clear();
        this.quoteLength.clear();
        this.quoteStack.clear();
    }
    
    private void setRecordEnd(final SMEvent event) {
        this.sm.canFlush(true);
        if (!this.inQuote) {
            this.setTimeStamp(event);
            this.setColumnData(event);
        }
        this.gotCompleteRecord = true;
        final long recBegin = this.logicalRecordBeginOffset;
        if (this.logicalRecordBeginOffset > this.lastRecordMark && this.logicalRecordBeginOffset > 0L) {
            this.recordBeginOffset += this.logicalRecordBeginOffset - this.lastRecordMark;
        }
        this.recordBeginOffset += this.trashedDataLen;
        this.lastRecordMark = event.currentPosition;
        this.unwantedDataLen = 0L;
        if (event.state() == 2) {
            final RowEvent rowEvent = (RowEvent)event;
            this.recordCheckpoint.setRecordBeginOffset(this.recordBeginOffset);
            this.recordCheckpoint.setRecordLength(rowEvent.position() - recBegin);
            this.recordBeginOffset += this.recordCheckpoint.getRecordLength();
            this.recordCheckpoint.setRecordEndOffset(this.recordBeginOffset);
            this.logicalRecordBeginOffset = event.currentPosition;
        }
        else {
            if (event.state() == 8) {
                this.recLength = event.position() - event.length - this.logicalRecordBeginOffset;
                this.logicalRecordBeginOffset = event.currentPosition - event.length;
                this.sm.rewind(event.length);
            }
            else if (event.state() == 11) {
                this.recLength = event.position() - this.logicalRecordBeginOffset;
                this.logicalRecordBeginOffset = event.currentPosition;
            }
            else if (event.state() != 7) {
                this.logger.warn((Object)("Only expect to have ROW_BEGIN or ROW_END evnet but we got {" + event.state() + "}.Please update the logic to handle it"));
            }
            this.trashedDataLen = 0L;
            this.rEvent = this.dummyRowEvent;
            this.recordCheckpoint.setRecordLength(this.recLength);
            this.recordCheckpoint.setRecordBeginOffset(this.recordBeginOffset);
            this.recordCheckpoint.setRecordEndOffset(this.recordBeginOffset + this.recLength);
            this.recordBeginOffset += this.recLength;
        }
        this.sm.lastRecordEndPosition((int)this.logicalRecordBeginOffset);
        if (this.beginOfTimeStamp != -1) {
            this.dateString = new String(this.rEvent.array(), this.beginOfTimeStamp, this.lengthOfTimeStamp);
            this.beginOfTimeStamp = -1;
        }
        this.setColumnCount(this.colCount + 0);
        ++this.recordCount;
        if (!this.inQuote) {
            this.hasValidRecord = true;
        }
        else {
            this.inQuote = false;
            this.hasValidRecord = false;
        }
        this.seenRecordBegin = false;
        this.sm.ignoreEvents(null);
        this.sm.canBreak(true);
    }
    
    private void setTimeStamp(final SMEvent event) {
        if (event.isDateEvent && ((!this.hasGotTimestamp && event.state() == 9) || (!this.seenRecordBegin && event.state() != 9))) {
            if (event.state() == 9) {
                this.hasGotTimestamp = true;
            }
            this.dateFormat = event.dateFormat;
            this.beginOfTimeStamp = event.position() - event.length();
            this.lengthOfTimeStamp = event.length();
            this.beginOfTimeStamp += event.prefixLength;
            this.lengthOfTimeStamp -= event.postfixLength;
        }
    }
    
    private boolean escapeEvent(final SMEvent event) {
        if (this.inEscape) {
            this.inEscape = false;
            if (this.escapeOffset == event.position() - event.length() && this.logger.isDebugEnabled()) {
                this.logger.debug((Object)"Escaping quote event");
            }
            return true;
        }
        return false;
    }
    
    @Override
    public void onEvent(final QuoteEvent event) {
        if (this.inEscape && this.escapeEvent(event)) {
            return;
        }
        this.seenQuote = true;
        if (this.inQuote) {
            this.inQuote = false;
            this.quoteLength.add(event.length());
            this.quoteEndPosition.add(event.position());
            this.sm.ignoreEvents(null);
            if (this.delimittedColumn) {
                this.sm.ignoreEvents(this.columnDelimitTillIgnoreList);
            }
        }
        else {
            this.quoteBeginPosition.add(event.position());
            this.inQuote = true;
            this.sm.ignoreEvents(this.quoteEventIgnoreList);
        }
    }
    
    @Override
    public void onEvent(final EndOfBlockEvent event) {
        if (this.colCount > 0 || this.columnOffset[0] > this.logicalRecordBeginOffset || event.position() > this.logicalRecordBeginOffset) {
            event.length = 0;
            this.setRecordEnd(event);
        }
        else {
            this.hasValidRecord = false;
            this.sm.canBreak(true);
        }
    }
    
    @Override
    public void onEvent(final RowEndEvent event) {
        if (this.seenRecordBegin) {
            this.seenRecordBegin = false;
            this.setRecordEnd(event);
        }
    }
    
    @Override
    public void onEvent(final QuoteBeginEvent event) {
        this.quoteBeginPosition.add(event.position());
        this.quoteStack.push(event);
        this.inQuote = true;
        this.sm.ignoreEvents(this.quoteSetEventIgnoreList, event.relatedEvent);
    }
    
    @Override
    public void onEvent(final QuoteEndEvent event) {
        this.quoteEndPosition.add(event.position());
        this.quoteLength.add(event.length());
        SMEvent beginEvent = this.quoteStack.pop();
        if (this.quoteStack.isEmpty()) {
            this.inQuote = false;
            this.seenQuote = true;
            this.sm.ignoreEvents(null);
        }
        else {
            beginEvent = this.quoteStack.peek();
            this.sm.ignoreEvents(this.quoteSetEventIgnoreList, beginEvent.relatedEvent);
        }
    }
    
    @Override
    public void onEvent(final RowBeginEvent event) {
        if (!this.seenRecordBegin) {
            if (this.hasGotRecordEnd) {
                this.sm.ignoreEvents(this.recordBeginIgnoreEventList);
            }
            this.setTimeStamp(event);
            this.seenRecordBegin = true;
            this.logicalRecordBeginOffset = event.currentPosition - event.length;
            if (this.lastRecordMark < this.logicalRecordBeginOffset) {
                this.unwantedDataLen = this.logicalRecordBeginOffset - this.lastRecordMark;
            }
            this.sm.lastRecordEndPosition((int)this.logicalRecordBeginOffset);
            this.colCount = 0;
            if (event.removePattern) {
                this.columnBeginOffset[this.colCount] = event.position();
            }
            else {
                this.columnBeginOffset[this.colCount] = event.position() - event.length();
            }
            this.sm.canFlush(false);
        }
        else {
            this.setRecordEnd(event);
        }
    }
    
    @Override
    public void onEvent(final TimeStampEvent event) {
        this.setTimeStamp(event);
    }
    
    @Override
    public void onEvent(final ResetEvent event) {
        this.trashedDataLen += event.currentPosition;
        this.trashedDataLen += this.unwantedDataLen;
        this.unwantedDataLen = 0L;
        this.rollOverFlag = true;
        this.recordBuffer = event.buffer;
        this.resetState();
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        this.colCount = 0;
        this.columnBeginOffset[0] = (int)this.logicalRecordBeginOffset;
        this.gotCompleteRecord = false;
        this.sm.canBreak(false);
        this.sm.canFlush(true);
        this.eventTime = null;
        this.dateString = null;
        this.hasGotTimestamp = false;
        this.rollOverFlag = false;
        this.seenRecordBegin = false;
        this.quoteStack.clear();
        try {
            while (!this.gotCompleteRecord) {
                this.sm.next();
            }
            if (this.resultSetMetadata != null && !this.resultSetMetadata.isValid) {
                this.resultSetMetadata = new CSVResultSetMetaData(this);
                return this.next();
            }
        }
        catch (RecordException rExp) {
            this.errorMessage = rExp.errMsg();
            if (rExp.returnStatus() != Constant.recordstatus.NO_RECORD) {
                this.logger.warn((Object)("Got RecordException : {" + this.errorMessage + "}"));
            }
            return rExp.returnStatus();
        }
        catch (AdapterException sExp) {
            if (sExp.getErrorMessage() != null) {
                this.errorMessage = sExp.getErrorMessage() + " data source :{" + this.reader().name() + "}";
            }
            else {
                this.errorMessage = "Got SourceException. Data source :{" + this.reader().name() + "}";
            }
            if (sExp.getType() == Error.END_OF_DATASOURCE) {
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)"Reached end of data source");
                }
                return Constant.recordstatus.END_OF_DATASOURCE;
            }
            if (this.isClosed) {
                this.logger.error((Object)("next() is called on a closed result-set. Exception : {" + this.errorMessage + "}"));
            }
            else {
                this.logger.warn((Object)("Got SourceException :{" + sExp.getMessage() + "}"));
            }
            return Constant.recordstatus.ERROR_RECORD;
        }
        catch (RuntimeException runExp) {
            final Throwable cause = runExp.getCause();
            if (cause != null && cause instanceof RecordException && ((RecordException)cause).type() == RecordException.Type.END_OF_DATASOURCE) {
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)"Reached end of data source");
                }
                return Constant.recordstatus.END_OF_DATASOURCE;
            }
            if (runExp == null || cause == null) {
                this.logger.warn((Object)"Null exception got while calling next()");
            }
            else {
                this.logger.warn((Object)"Got exception while calling next()", (Throwable)runExp);
                this.logger.warn((Object)"The cause ", cause);
            }
        }
        catch (Exception exp) {
            exp.printStackTrace();
            return Constant.recordstatus.NO_RECORD;
        }
        if (this.hasValidRecord) {
            this.eventTimestamp = System.currentTimeMillis();
            if (!this.firstEvent) {
                ReaderMetadataPersistenceUtility.persistFirstEventTimestamp(this.dataSource, this.eventTimestamp);
                this.firstEvent = true;
            }
            return Constant.recordstatus.VALID_RECORD;
        }
        this.logger.warn((Object)("Invalid Record is encountered at : [" + this.getCheckpointDetail().toString() + "]"));
        this.clearInternalState();
        return Constant.recordstatus.INVALID_RECORD;
    }
    
    private void clearInternalState() {
        this.quoteStack.clear();
        this.quoteEndPosition.clear();
        this.quoteBeginPosition.clear();
        this.quoteLength.clear();
    }
    
    @Override
    public String getColumnValue(final int colIdx) {
        final String tmp = new String(this.recordBuffer, this.columnBeginOffset[colIdx], this.columnOffset[colIdx] - this.columnBeginOffset[colIdx]);
        if (this.csvProp.trimwhitespace) {
            return tmp.trim();
        }
        return tmp;
    }
    
    @Override
    public MetaData getMetaData() {
        return this.resultSetMetadata;
    }
    
    @Override
    public Date eventTime() {
        if (this.dateString != null) {
            SimpleDateFormat dateFormater = this.dateFormaterMap.get(this.dateFormat);
            if (dateFormater == null) {
                dateFormater = new SimpleDateFormat(this.dateFormat);
                this.dateFormaterMap.put(this.dateFormat, dateFormater);
            }
            try {
                this.eventTime = dateFormater.parse(this.dateString);
            }
            catch (ParseException e) {
                this.logger.warn((Object)("Couldn't convert [" + this.dateString + "] the text using [" + dateFormater.toPattern().toString() + "] Record # : [" + this.recordCount + "]"));
                return null;
            }
            return this.eventTime;
        }
        return null;
    }
    
    @Override
    public void reset() throws AdapterException {
        boolean invalidateBuffer = false;
        this.positionToBeginning();
        if (this.recordCheckpoint != null) {
            invalidateBuffer = (this.reader().skipBytes(this.recordCheckpoint.seekPosition()) != 0L);
        }
        this.sm.reset(invalidateBuffer);
        if (invalidateBuffer) {
            this.recordBeginOffset = 0L;
        }
    }
    
    @Override
    public void update(final Observable o, final Object arg) {
        switch ((com.datasphere.source.lib.constant.Constant.eventType)arg) {
            case ON_OPEN: {
                this.setRecordCount(0);
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)(((ReaderBase)o).name() + " is opened "));
                }
                this.sourceCheckpoint.seekPosition(0L);
                this.sourceCheckpoint.setSourceName(this.dataSource.name());
                this.recordBeginOffset = 0L;
                this.lineSkipCount = 0;
                if (!this.csvProp.header && this.csvProp.headerlineno == 0) {
                    break;
                }
                if (this.resultSetMetadata != null) {
                    this.resultSetMetadata.isValid = false;
                }
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)"New file is opened and header will be skipped");
                    break;
                }
                break;
            }
            case ON_CLOSE: {
                ReaderMetadataPersistenceUtility.persistEventCountAndLastEventTimestamp(this.dataSource, this.getRecordCount(), this.eventTimestamp);
                this.eventTimestamp = 0L;
                this.firstEvent = false;
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)("No of records in the file is " + this.getRecordCount()));
                }
                if (((ReaderBase)o).name() != null && this.logger.isInfoEnabled()) {
                    this.logger.info((Object)(((ReaderBase)o).name() + " is closed\n"));
                    break;
                }
                break;
            }
        }
    }
    
    @Override
    public Map<String, String> getColumnValueAsMap(final int index) throws AdapterException {
        final NameValueParser nvp = new NameValueParser(new NVPProperty(this.csvProp.getMap()));
        String columnValue = this.getColumnValue(index);
        if (columnValue.charAt(0) == this.csvProp.quotecharacter && columnValue.charAt(columnValue.length() - 1) == this.csvProp.quotecharacter) {
            columnValue = columnValue.substring(1, columnValue.length() - 1);
            return nvp.convertToMap(columnValue);
        }
        return nvp.convertToMap(columnValue);
    }
    
    @Override
    public void close() throws AdapterException {
        this.sm.close();
        ReaderMetadataPersistenceUtility.persistEventCountAndLastEventTimestamp(this.dataSource, this.getRecordCount(), this.eventTimestamp);
        this.eventTimestamp = 0L;
        super.close();
        if (this.quoteStack != null) {
            this.quoteStack.clear();
        }
        if (this.dummyRowEvent != null) {
            this.dummyRowEvent = null;
        }
        if (this.recordBuffer != null) {
            this.recordBuffer = null;
        }
    }
}
