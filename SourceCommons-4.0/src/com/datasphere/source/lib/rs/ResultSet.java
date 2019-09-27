package com.datasphere.source.lib.rs;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.intf.SMCallback;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.smlite.ColumnEvent;
import com.datasphere.source.smlite.CommentEvent;
import com.datasphere.source.smlite.EndOfBlockEvent;
import com.datasphere.source.smlite.EscapeEvent;
import com.datasphere.source.smlite.NVPEvent;
import com.datasphere.source.smlite.QuoteBeginEvent;
import com.datasphere.source.smlite.QuoteEndEvent;
import com.datasphere.source.smlite.QuoteEvent;
import com.datasphere.source.smlite.ResetEvent;
import com.datasphere.source.smlite.RowBeginEvent;
import com.datasphere.source.smlite.RowEndEvent;
import com.datasphere.source.smlite.RowEvent;
import com.datasphere.source.smlite.SMEvent;
import com.datasphere.source.smlite.TimeStampEvent;

public abstract class ResultSet implements Observer, SMCallback
{
    private char[] rowMetaData;
    private int columnCount;
    protected int recordCount;
    protected Property prop;
    boolean matchFound;
    int firstColumnEndPos;
    Reader reader;
    protected String errorMessage;
    protected boolean isClosed;
    protected CheckpointDetail sourceCheckpoint;
    protected CheckpointDetail recordCheckpoint;
    protected int[] columnOffset;
    protected int[] columnBeginOffset;
    protected int[] columnLength;
    Logger logger;
    
    public ResultSet(final Reader reader, final Property prop) throws IOException, InterruptedException {
        this.columnCount = 0;
        this.recordCount = 0;
        this.matchFound = false;
        this.firstColumnEndPos = 0;
        this.logger = Logger.getLogger((Class)ResultSet.class);
        this.prop = prop;
        this.reader = reader;
    }
    
    protected void Init() throws IOException, InterruptedException {
        this.columnOffset = new int[this.prop.maxcolumnoffset];
        this.columnLength = new int[this.prop.maxcolumnoffset];
        this.columnBeginOffset = new int[this.prop.maxcolumnoffset];
        this.setFirstColumnData(null);
        this.firstColumnEndPos = this.getRowDataFirstColumnEndPos();
        this.rowMetaData = new char[this.reader.blockSize()];
        this.recordCheckpoint = new CheckpointDetail(this.reader.getCheckpointDetail());
        this.sourceCheckpoint = new CheckpointDetail(this.reader.getCheckpointDetail());
    }
    
    public Reader reader() {
        return this.reader;
    }
    
    public int getRowDataFirstColumnEndPos() {
        return this.firstColumnEndPos;
    }
    
    public void setRowDataFirstColumnEndPos(final int firstColumnEndPos) {
        this.firstColumnEndPos = firstColumnEndPos;
    }
    
    public void setFirstColumnData(final String fileName) {
    }
    
    public String getColumnName(final int columnIndex) {
        return String.valueOf(this.rowMetaData, this.columnOffset[columnIndex], this.columnLength[columnIndex]);
    }
    
    public void close() throws AdapterException {
        try {
            this.isClosed = true;
            this.reader.close();
            this.columnOffset = null;
            this.columnBeginOffset = null;
            this.columnLength = null;
            this.rowMetaData = null;
        }
        catch (IOException exp) {
            throw new AdapterException("Problem closing the reader " + this.reader.name(), (Throwable)exp);
        }
    }
    
    public abstract Constant.recordstatus next() throws IOException, InterruptedException;
    
    public int getColumnCount() {
        return this.columnCount;
    }
    
    public int getRecordCount() {
        return this.recordCount;
    }
    
    public void setRecordCount(final int recordcount) {
        this.recordCount = recordcount;
    }
    
    public void setColumnCount(final int columncount) {
        this.columnCount = columncount;
    }
    
    public String getCurrentFile() {
        return this.reader.name();
    }
    
    public void setColumnOffset(final int columnIndex, final int columnOffset) {
        this.columnOffset[columnIndex] = columnOffset;
    }
    
    public void setColumnLength(final int columnIndex, final int columnLength) {
        this.columnLength[columnIndex] = columnLength;
    }
    
    @Override
    public void update(final Observable o, final Object arg) {
        switch ((com.datasphere.source.lib.constant.Constant.eventType)arg) {
            case ON_OPEN: {
                this.recordCheckpoint.setSourceName(this.reader().name());
                break;
            }
            case ON_CLOSE: {
                this.recordCheckpoint.setSourceName("");
                break;
            }
        }
    }
    
    public CheckpointDetail getCheckpointDetail() {
        final CheckpointDetail checkpointDetail = new CheckpointDetail(this.recordCheckpoint);
        checkpointDetail.setBytesRead(this.reader.getCheckpointDetail().getBytesRead());
        return checkpointDetail;
    }
    
    public void positionToBeginning() throws AdapterException {
        this.reader().skipBytes(0L);
    }
    
    public void reset() throws AdapterException {
    }
    
    public MetaData getMetaData() {
        return null;
    }
    
    public Map<String, String> getColumnValueAsMap(final int index) throws AdapterException {
        return null;
    }
    
    public List<String> applyRegexOnColumnValue(final String columnValue, final String regex) {
        return null;
    }
    
    public Constant.recordstatus getRecordStatus() {
        return null;
    }
    
    public String getErrorMessage() {
        return this.errorMessage;
    }
    
    public Date eventTime() {
        return null;
    }
    
    @Override
    public void onEvent(final ResetEvent event) {
    }
    
    @Override
    public void onEvent(final RowEvent event) {
    }
    
    @Override
    public void onEvent(final ColumnEvent event) {
    }
    
    @Override
    public void onEvent(final QuoteEvent event) {
    }
    
    @Override
    public void onEvent(final QuoteBeginEvent event) {
    }
    
    @Override
    public void onEvent(final QuoteEndEvent event) {
    }
    
    @Override
    public boolean onEvent(final SMEvent eventData) {
        return false;
    }
    
    @Override
    public void onEvent(final RowBeginEvent event) {
    }
    
    @Override
    public void onEvent(final RowEndEvent event) {
    }
    
    @Override
    public void onEvent(final TimeStampEvent event) {
    }
    
    @Override
    public void onEvent(final EndOfBlockEvent event) {
    }
    
    @Override
    public void onEvent(final EscapeEvent event) {
    }
    
    @Override
    public void onEvent(final CommentEvent event) {
    }
    
    @Override
    public void onEvent(final NVPEvent nvpEvent) {
    }
}
