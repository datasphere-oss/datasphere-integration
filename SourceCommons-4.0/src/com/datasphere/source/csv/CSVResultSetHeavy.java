package com.datasphere.source.csv;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.reader.ReaderBase;
import com.datasphere.source.lib.rs.CharResultSet;
import com.datasphere.source.nvp.NVPProperty;
import com.datasphere.source.nvp.NameValueParser;
import com.datasphere.source.sm.QuoteSet;
import com.datasphere.source.sm.SMProperty;
import com.datasphere.source.sm.StateMachine;

public class CSVResultSetHeavy extends CharResultSet
{
    Logger logger;
    CSVResultSetMetaData resultSetMetadata;
    StateMachine stateMachine;
    int rowEndOffset;
    int firstColumnEndPos;
    public long rowStartOffset;
    int[][] stateDecider;
    Constant.recordstatus rstatus;
    CSVProperty csvProp;
    List<String> matchedString;
    int columnDelimitTill;
    long fileOffset;
    int lineNumber;
    int lineOffset;
    CharBuffer buffer;
    CharBuffer leftOverBuffer;
    boolean ifFileChangedFetchMetadata;
    
    public CSVResultSetHeavy(final Reader reader, final CSVProperty csvProp) throws IOException, InterruptedException, AdapterException {
        super(reader, csvProp);
        this.logger = Logger.getLogger((Class)CSVResultSetHeavy.class);
        this.resultSetMetadata = null;
        this.rowEndOffset = 0;
        this.firstColumnEndPos = 0;
        this.rowStartOffset = 0L;
        this.matchedString = null;
        this.columnDelimitTill = -1;
        this.fileOffset = 0L;
        this.lineNumber = 1;
        this.lineOffset = 0;
        this.ifFileChangedFetchMetadata = false;
        this.Init();
        this.leftOverBuffer = CharBuffer.allocate(this.reader().blockSize());
        (this.buffer = CharBuffer.allocate(this.reader().blockSize() * 2)).flip();
        this.leftOverBuffer.flip();
        this.matchedString = new LinkedList<String>();
        this.csvProp = csvProp;
        if (csvProp.headerlineno != 0) {
            this.lineOffset = csvProp.headerlineno;
        }
        if (csvProp.lineoffset != 0) {
            this.lineOffset = csvProp.lineoffset;
        }
        this.sourceCheckpoint = this.reader().getCheckpointDetail();
        this.recordCheckpoint = this.sourceCheckpoint;
        this.stateMachine = new StateMachine(new SMProperty(csvProp.propMap));
        if (csvProp.getBoolean("header", false) && !this.recordCheckpoint.isRecovery()) {
            this.resultSetMetadata = new CSVResultSetMetaData(this, csvProp);
        }
        this.columnDelimitTill = csvProp.columnDelimitTill;
        this.reader().registerObserver(this);
        if (this.sourceCheckpoint.isRecovery()) {
            this.fileOffset = this.sourceCheckpoint.getRecordEndOffset();
        }
    }
    
    @Override
    public CSVResultSetMetaData getMetaData() {
        return this.resultSetMetadata;
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)("Record count is " + this.getRecordCount()));
        }
        int rowEnd = 0;
        int colCount = 0;
        com.datasphere.source.lib.constant.Constant.status retStatus = com.datasphere.source.lib.constant.Constant.status.NORMAL;
        int begin = 0;
        int end = 0;
        int i = 0;
        int leftOverRowSize = 0;
        int offsetAdjustment = 0;
        int columnLength = -1;
        if (this.ifFileChangedFetchMetadata) {
            this.ifFileChangedFetchMetadata = false;
            final Constant.recordstatus rs = this.next();
            if (rs == Constant.recordstatus.VALID_RECORD) {
                this.resultSetMetadata = new CSVResultSetMetaData(this);
            }
        }
        if (this.buffer.position() < this.buffer.limit()) {
            int rowBegin = this.buffer.position();
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Buffer Postion is :" + this.buffer.position() + "Block Size is :" + this.reader().blockSize() + "\n"));
            }
            if (this.getColumnCount() != 0) {
                this.setColumnCount(0);
            }
            if (this.buffer.position() != this.buffer.limit()) {
                ++colCount;
                final int rowDataFirstColumnEndPos = this.getRowDataFirstColumnEndPos();
                this.firstColumnEndPos = rowDataFirstColumnEndPos;
                begin = rowDataFirstColumnEndPos;
                final String rowstartOffset = Long.toString(this.fileOffset);
                i = (end = i + (rowstartOffset.length() + this.firstColumnEndPos));
                this.setColumnOffset(colCount, begin);
                this.setColumnLength(colCount, end - begin);
                begin = i;
                ++colCount;
                rowstartOffset.getChars(0, rowstartOffset.length(), this.rowData, this.firstColumnEndPos);
                this.firstColumnEndPos = begin;
            }
            while (this.buffer.position() != this.buffer.limit() || this.stateMachine.eventQueue.size() != 0) {
                if (this.stateMachine.eventQueue.size() != 0) {
                    retStatus = this.stateMachine.eventQueue.remove();
                }
                else {
                    final char c = this.buffer.get();
                    retStatus = this.stateMachine.process(c);
                    if (retStatus == com.datasphere.source.lib.constant.Constant.status.MULTIPLE_STATUS) {
                        retStatus = this.stateMachine.eventQueue.remove();
                        offsetAdjustment = 0;
                    }
                    else {
                        offsetAdjustment = 1;
                    }
                }
                if (retStatus == com.datasphere.source.lib.constant.Constant.status.END_OF_COLUMN) {
                    if (this.lineOffset != 0 && this.lineNumber < this.lineOffset) {
                        continue;
                    }
                    end = i - this.prop.columndelimiterlist.length();
                    if (this.columnDelimitTill == -1 || this.columnDelimitTill > colCount) {
                        this.setColumnOffset(colCount, begin);
                        this.setColumnLength(colCount, end - begin);
                        int offset = 0;
                        int len = 0;
                        final QuoteSet quoteSet = this.stateMachine.matchedQuoteSet();
                        if (this.csvProp.trimQuote && quoteSet != null) {
                            final String matchedQuote = quoteSet.toString();
                            len = matchedQuote.length();
                            if (len == 1) {
                                if (this.buffer.array()[this.buffer.position() - 3 - (end - begin - 1)] == matchedQuote.charAt(0) && this.buffer.array()[this.buffer.position() - 3] == matchedQuote.charAt(0)) {
                                    offset = 1;
                                }
                            }
                            else if (this.buffer.array()[this.buffer.position() - 3 - (end - begin - 1)] == matchedQuote.charAt(0) && this.buffer.array()[this.buffer.position() - 3] == matchedQuote.charAt(1)) {
                                offset = 1;
                            }
                        }
                        columnLength = end - begin + -1 * offset * 2;
                        if (!this.csvProp.ignoreEmptyColums || columnLength != 0) {
                            this.setColumnOffset(colCount, begin + offset);
                            this.setColumnLength(colCount, columnLength);
                            ++colCount;
                        }
                        begin = end + this.prop.columndelimiterlist.length();
                    }
                    ++i;
                }
                else if (retStatus == com.datasphere.source.lib.constant.Constant.status.END_OF_ROW) {
                    ++this.lineNumber;
                    if (this.lineOffset == 0 || this.lineNumber > this.lineOffset) {
                        rowEnd = this.buffer.position();
                        final int rowLength = rowEnd - rowBegin;
                        this.buffer.position(rowBegin);
                        this.buffer.get(this.rowData, this.firstColumnEndPos, rowLength);
                        end = i - this.prop.rowdelimiterlist.length() + offsetAdjustment;
                        int offset2 = 0;
                        int len2 = 0;
                        final QuoteSet quoteSet2 = this.stateMachine.matchedQuoteSet();
                        final String recDel = this.stateMachine.matchedRowDelimiter();
                        if (this.csvProp.trimQuote && quoteSet2 != null) {
                            final String matchedQuote2 = quoteSet2.toString();
                            len2 = matchedQuote2.length();
                            if (len2 == 1) {
                                if (this.buffer.array()[this.buffer.position() - 2 - (end - begin - 1)] == matchedQuote2.charAt(0) && this.buffer.array()[this.buffer.position() - (recDel.length() + 1)] == matchedQuote2.charAt(0)) {
                                    offset2 = 1;
                                }
                            }
                            else if (this.buffer.array()[this.buffer.position() - 2 - (end - begin - 1)] == matchedQuote2.charAt(0) && this.buffer.array()[this.buffer.position() - (recDel.length() + 1)] == matchedQuote2.charAt(1)) {
                                offset2 = 1;
                            }
                        }
                        columnLength = end - begin + -1 * offset2 * (recDel.length() + 1);
                        if (!this.csvProp.ignoreEmptyColums || columnLength != 0) {
                            this.setColumnOffset(colCount, begin + offset2);
                            this.setColumnLength(colCount, columnLength);
                            ++colCount;
                        }
                        this.setColumnCount(colCount);
                        this.setRecordCount(this.getRecordCount() + 1);
                        if (this.recordCheckpoint != null) {
                            this.recordCheckpoint.setRecordBeginOffset(this.fileOffset);
                            this.recordCheckpoint.setRecordLength((long)rowLength);
                        }
                        this.rowEndOffset = this.firstColumnEndPos + rowLength;
                        this.rowStartOffset += this.rowEndOffset;
                        this.fileOffset += rowLength;
                        if (this.stateMachine.getCurrentState() == this.stateMachine.getQuotedCharState()) {
                            this.rstatus = Constant.recordstatus.INVALID_RECORD;
                            this.stateMachine.reset();
                        }
                        else {
                            this.rstatus = Constant.recordstatus.VALID_RECORD;
                        }
                        if (this.recordCheckpoint != null) {
                            this.recordCheckpoint.setRecordEndOffset(this.fileOffset);
                        }
                        return this.rstatus;
                    }
                    rowBegin = this.buffer.position();
                }
                else if (retStatus == com.datasphere.source.lib.constant.Constant.status.NORMAL) {
                    if (this.lineOffset != 0 && this.lineNumber < this.lineOffset) {
                        continue;
                    }
                    ++i;
                }
                else {
                    if (retStatus == com.datasphere.source.lib.constant.Constant.status.IN_COMMENT) {
                        continue;
                    }
                    if (retStatus != com.datasphere.source.lib.constant.Constant.status.END_OF_COMMENT) {
                        this.stateMachine.reset();
                        return this.rstatus = Constant.recordstatus.ERROR_RECORD;
                    }
                    ++this.lineNumber;
                    rowBegin = this.buffer.position();
                }
            }
            leftOverRowSize = this.buffer.limit() - rowBegin;
            this.buffer.position(rowBegin);
            if (leftOverRowSize != 0) {
                this.leftOverBuffer.clear();
                this.leftOverBuffer.flip();
                if (leftOverRowSize > this.leftOverBuffer.capacity()) {
                    this.logger.error((Object)"BUFFER_LIMIT_EXCEED_ERROR - Record size is greater than block size");
                }
                this.buffer.get(this.leftOverBuffer.array(), 0, leftOverRowSize);
                this.buffer.clear();
            }
            else {
                this.buffer.clear();
            }
        }
        CharBuffer tmpBuffer = null;
        try {
            tmpBuffer = (CharBuffer)this.reader().readBlock();
        }
        catch (AdapterException se) {
            if (se.getType() == Error.END_OF_DATASOURCE) {
                return Constant.recordstatus.END_OF_DATASOURCE;
            }
            se.printStackTrace();
        }
        if (tmpBuffer == null) {
            if (leftOverRowSize != 0) {
                this.buffer.put(this.leftOverBuffer.array(), 0, leftOverRowSize);
                this.buffer.position(leftOverRowSize);
                this.leftOverBuffer.clear();
                leftOverRowSize = 0;
                this.buffer.flip();
            }
            else {
                this.buffer.clear();
                this.buffer.limit(0);
            }
            return this.rstatus = Constant.recordstatus.NO_RECORD;
        }
        this.buffer.clear();
        if (leftOverRowSize != 0) {
            this.buffer.put(this.leftOverBuffer.array(), 0, leftOverRowSize);
            this.buffer.position(leftOverRowSize);
            leftOverRowSize = 0;
        }
        this.buffer.put(tmpBuffer.array(), 0, tmpBuffer.limit());
        this.buffer.flip();
        this.stateMachine.reset();
        return this.next();
    }
    
    @Override
    public void setFirstColumnData(final String name) {
        int i = 0;
        int end = 0;
        int colCount = 0;
        int begin = 0;
        String fileName = name;
        if (fileName == null) {
            fileName = this.reader().name();
        }
        if (fileName != null) {
            i = (end = fileName.length());
            this.setColumnOffset(colCount, begin);
            this.setColumnLength(colCount, end - begin);
            begin = i;
            ++colCount;
            fileName.getChars(0, fileName.length(), this.rowData, 0);
        }
        this.setRowDataFirstColumnEndPos(begin);
    }
    
    @Override
    public Constant.recordstatus getRecordStatus() {
        return this.rstatus;
    }
    
    public long getFileOffset() {
        return this.fileOffset;
    }
    
    public long getRowStartOffset() {
        return this.rowStartOffset;
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
    public List<String> applyRegexOnColumnValue(final String columnValue, final String regex) {
        this.matchedString.clear();
        final Pattern pattern = Pattern.compile(regex, 2);
        final Matcher patternMatcher = pattern.matcher(columnValue);
        while (patternMatcher.find()) {
            this.matchedString.add(patternMatcher.group());
        }
        return this.matchedString;
    }
    
    @Override
    public void update(final Observable o, final Object arg) {
        switch ((com.datasphere.source.lib.constant.Constant.eventType)arg) {
            case ON_OPEN: {
                this.setRecordCount(0);
                this.lineNumber = 1;
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)(((ReaderBase)o).name() + " is opened "));
                }
                this.setFirstColumnData(((ReaderBase)o).name());
                this.rowStartOffset = 0L;
                this.fileOffset = 0L;
                this.sourceCheckpoint.seekPosition(0L);
                if (this.csvProp.header || this.csvProp.headerlineno != 0) {
                    this.ifFileChangedFetchMetadata = true;
                    break;
                }
                break;
            }
            case ON_CLOSE: {
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
    public void reset() throws AdapterException {
        this.positionToBeginning();
        this.reader().skipBytes(this.sourceCheckpoint.seekPosition());
        this.buffer.clear();
        this.buffer.flip();
        this.stateMachine.reset();
        this.rowEndOffset = 0;
        this.rowStartOffset = 0L;
        this.fileOffset = 0L;
    }
}
