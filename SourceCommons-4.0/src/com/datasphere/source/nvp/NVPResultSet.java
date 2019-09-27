package com.datasphere.source.nvp;

import java.nio.*;
import org.apache.log4j.*;

import com.datasphere.common.constants.*;

import java.io.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.reader.*;
import com.datasphere.source.lib.rs.*;
import com.datasphere.source.nvp.sm.*;
import com.datasphere.source.smlite.*;

public class NVPResultSet extends CharResultSet
{
    CharBuffer buffer;
    Logger logger;
    NVPStateMachine stateMachine;
    int rowEndOffset;
    int firstColumnEndPos;
    public long rowStartOffset;
    int[][] stateDecider;
    Constant.recordstatus rstatus;
    int prevRowEndOffset;
    int columnDelimitTill;
    boolean endOfResultSet;
    int lineNumber;
    
    public NVPResultSet(final NVPProperty prop) throws IOException, InterruptedException {
        super(null, prop);
        this.logger = Logger.getLogger((Class)NVPResultSet.class);
        this.rowEndOffset = 0;
        this.firstColumnEndPos = 0;
        this.rowStartOffset = 0L;
        this.prevRowEndOffset = 0;
        this.columnDelimitTill = -1;
        this.lineNumber = 1;
        this.Init();
        this.stateMachine = new NVPStateMachine(prop);
        this.buffer = CharBuffer.allocate(1048576);
    }
    
    @Override
    protected void Init() {
        this.columnOffset = new int[this.prop.maxcolumnoffset];
        this.columnLength = new int[this.prop.maxcolumnoffset];
        this.rowData = new char[1048576];
    }
    
    public void setBuffer(final String msg) {
        this.buffer.put(msg);
        this.buffer.limit(this.buffer.position());
        this.buffer.rewind();
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        if (this.logger.isDebugEnabled() && this.logger.isDebugEnabled()) {
            this.logger.debug((Object)("Record count is " + this.getRecordCount()));
        }
        int rowEnd = 0;
        int colCount = 0;
        com.datasphere.source.lib.constant.Constant.status retStatus = com.datasphere.source.lib.constant.Constant.status.NORMAL;
        int begin = 0;
        int end = 0;
        int i = 0;
        if (this.endOfResultSet) {
            return Constant.recordstatus.ERROR_RECORD;
        }
        int rowBegin = this.buffer.position();
        if (this.getColumnCount() != 0) {
            this.setColumnCount(0);
        }
        boolean breakLoop = false;
        while (!this.endOfResultSet) {
            if (breakLoop) {
                breakLoop = false;
                break;
            }
            if (this.buffer.position() == this.buffer.limit()) {
                retStatus = com.datasphere.source.lib.constant.Constant.status.END_OF_ROW;
                this.endOfResultSet = true;
            }
            else if (this.stateMachine.eventQueue.size() != 0) {
                retStatus = this.stateMachine.eventQueue.remove();
            }
            else {
                final char c = this.buffer.get();
                retStatus = this.stateMachine.process(c);
                if (retStatus == com.datasphere.source.lib.constant.Constant.status.MULTIPLE_STATUS) {
                    retStatus = this.stateMachine.eventQueue.remove();
                }
            }
            if (retStatus == com.datasphere.source.lib.constant.Constant.status.END_OF_COLUMN) {
                if (this.prop.lineoffset != 0 && this.lineNumber < this.prop.lineoffset) {
                    continue;
                }
                end = i;
                --end;
                this.setColumnOffset(colCount, begin);
                this.setColumnLength(colCount, end - begin);
                begin = end + 1;
                ++colCount;
                ++i;
            }
            else {
                if (retStatus == com.datasphere.source.lib.constant.Constant.status.END_OF_ROW) {
                    ++this.lineNumber;
                    rowEnd = this.buffer.position();
                    int delimiterLength = 0;
                    if (!this.endOfResultSet) {
                        delimiterLength = this.stateMachine.getRowDelimiterProcessingState().getMatchedDelimiterLength();
                    }
                    final int rowLength = rowEnd - rowBegin - delimiterLength;
                    this.buffer.position(rowBegin);
                    this.buffer.get(this.rowData, this.firstColumnEndPos, rowLength);
                    this.buffer.position(rowEnd);
                    end = rowLength;
                    this.setColumnOffset(colCount, begin);
                    this.setColumnLength(colCount, end - begin);
                    ++colCount;
                    this.setColumnCount(colCount);
                    this.setRecordCount(this.getRecordCount() + 1);
                    this.rowEndOffset = this.firstColumnEndPos + rowLength;
                    this.rowStartOffset += this.rowEndOffset;
                    return this.rstatus = Constant.recordstatus.VALID_RECORD;
                }
                if (retStatus == com.datasphere.source.lib.constant.Constant.status.NORMAL) {
                    ++i;
                }
                else {
                    if (retStatus == com.datasphere.source.lib.constant.Constant.status.IN_COMMENT) {
                        continue;
                    }
                    if (retStatus != com.datasphere.source.lib.constant.Constant.status.END_OF_COMMENT) {
                        final long rowBeginFileOffset = 0L;
                        final long rowCurrentFileOffset = 0L;
                        this.stateMachine.reset();
                        return this.rstatus = Constant.recordstatus.ERROR_RECORD;
                    }
                    ++this.lineNumber;
                    rowBegin = this.buffer.position();
                }
            }
        }
        return Constant.recordstatus.ERROR_RECORD;
    }
    
    public void setFirstColumnData(final File file) {
        int i = 0;
        int end = 0;
        int colCount = 0;
        int begin = 0;
        final String fileName = "test";
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
    
    public long getRowStartOffset() {
        return this.rowStartOffset;
    }
    
    public void handleEvent(final com.datasphere.source.lib.constant.Constant.eventType fileEvent, final File file) throws IOException {
        switch (fileEvent) {
            case ON_OPEN: {
                this.setRecordCount(0);
                this.lineNumber = 1;
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)(file.getName() + " is opened "));
                }
                this.setFirstColumnData(file);
                this.rowStartOffset = 0L;
                break;
            }
            case ON_CLOSE: {
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)("No of records in the file is " + this.getRecordCount()));
                    break;
                }
                break;
            }
        }
    }
    
    @Override
    public void onEvent(final QuoteEvent qEvent) {
    }
    
    @Override
    public void onEvent(final RowBeginEvent event) {
    }
    
    @Override
    public void onEvent(final TimeStampEvent event) {
    }
}
