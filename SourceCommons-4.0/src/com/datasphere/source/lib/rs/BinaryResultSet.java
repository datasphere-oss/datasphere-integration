package com.datasphere.source.lib.rs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Observable;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.meta.Column;
import com.datasphere.source.lib.meta.MetaData;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.reader.ReaderBase;
import com.datasphere.source.lib.utils.ByteUtil;

public abstract class BinaryResultSet extends ResultSet
{
    protected byte[] rowData;
    Logger logger;
    MetaData meta;
    Column[] columns;
    ByteBuffer internalBuffer;
    int recordBegin;
    int stringLength;
    long recordBeginOffset;
    long recordEndOffset;
    
    public BinaryResultSet(final Reader reader, final Property prop) throws IOException, InterruptedException {
        super(reader, prop);
        this.logger = Logger.getLogger((Class)BinaryResultSet.class);
        this.recordBeginOffset = 0L;
        this.recordEndOffset = 0L;
    }
    
    public void metaData(final MetaData meta) {
        this.meta = meta;
        this.columns = meta.colums();
    }
    
    @Override
    protected void Init() throws IOException, InterruptedException {
        this.rowData = new byte[this.reader().blockSize()];
        (this.internalBuffer = ByteBuffer.allocate(this.reader().blockSize() * 2)).flip();
        super.Init();
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        this.recordBegin = this.internalBuffer.position();
        int colIdx;
        while (true) {
            for (colIdx = 0; colIdx < this.columns.length; ++colIdx) {
                if (this.internalBuffer.position() >= this.internalBuffer.limit()) {
                    break;
                }
                this.columnOffset[colIdx] = this.internalBuffer.position();
                if (this.columns[colIdx].getType() != com.datasphere.source.lib.constant.Constant.fieldType.STRING) {
                    this.columnLength[colIdx] = this.columns[colIdx].getSize();
                }
                else {
                    if (this.internalBuffer.limit() - this.internalBuffer.position() < 2) {
                        break;
                    }
                    this.stringLength = ByteUtil.bytesToShort(this.internalBuffer.array(), this.columnOffset[colIdx]);
                    this.stringLength -= this.columns[colIdx].lengthOffset();
                    if (this.internalBuffer.position() + this.stringLength > this.internalBuffer.limit()) {
                        break;
                    }
                    this.columnLength[colIdx] = this.stringLength;
                }
                if (this.columnOffset[colIdx] + this.columnLength[colIdx] > this.internalBuffer.limit()) {
                    break;
                }
                this.internalBuffer.position(this.columnOffset[colIdx] + this.columnLength[colIdx]);
            }
            if (colIdx < this.columns.length) {
                if (this.recordBegin != 0) {
                    this.internalBuffer.position(this.recordBegin);
                    final int sizeToCopy = this.internalBuffer.limit() - this.recordBegin;
                    if (sizeToCopy > 0) {
                        System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), this.internalBuffer.array(), 0, sizeToCopy);
                    }
                    this.internalBuffer.limit(sizeToCopy);
                    this.internalBuffer.position(0);
                    this.recordBegin = 0;
                }
                else {
                    this.internalBuffer.position(this.recordBegin);
                }
                try {
                    final ByteBuffer tmpBuffer = (ByteBuffer)this.reader.readBlock();
                    if (tmpBuffer != null) {
                        System.arraycopy(tmpBuffer.array(), 0, this.internalBuffer.array(), this.internalBuffer.limit(), tmpBuffer.limit());
                        this.internalBuffer.limit(this.internalBuffer.limit() + tmpBuffer.limit());
                        this.internalBuffer.position(0);
                        continue;
                    }
                    return Constant.recordstatus.NO_RECORD;
                }
                catch (AdapterException e) {
                    e.printStackTrace();
                    return Constant.recordstatus.NO_RECORD;
                }
            }
            break;
        }
        this.setColumnCount(colIdx);
        --colIdx;
        this.recordCheckpoint.setRecordBeginOffset((long)((this.recordCheckpoint.getRecordEndOffset() == null) ? this.sourceCheckpoint.getRecordEndOffset() : this.recordCheckpoint.getRecordEndOffset()));
        this.recordEndOffset += this.columnOffset[colIdx] + this.columnLength[colIdx] - this.columnOffset[0];
        this.recordCheckpoint.setRecordEndOffset(this.sourceCheckpoint.getRecordEndOffset() + this.recordEndOffset);
        return Constant.recordstatus.VALID_RECORD;
    }
    
    public Object getColumnValue(final int columnIndex) {
        if (this.columnLength[columnIndex] != 0 && this.columnOffset[columnIndex] != -1) {
            final Object value = this.columns[columnIndex].getValue(this.internalBuffer.array(), this.columnOffset[columnIndex], this.columnLength[columnIndex]);
            Object retValue = null;
            switch (this.columns[columnIndex].getType()) {
                case BYTE: {
                    retValue = value;
                    break;
                }
                case DOUBLE: {
                    retValue = value;
                    break;
                }
                case FLOAT: {
                    retValue = value;
                    break;
                }
                case LONG: {
                    retValue = value;
                    break;
                }
                case INTEGER: {
                    retValue = value;
                    break;
                }
                case SHORT: {
                    retValue = value;
                    break;
                }
                case STRING: {
                    retValue = value;
                    break;
                }
                default: {
                    System.out.println("Unhandled data type.");
                    retValue = null;
                    break;
                }
            }
            return retValue;
        }
        return "";
    }
    
    public int copyColumnValue(final byte[] buffer, final int offset, final int columnIndex) {
        if (buffer.length < this.columnLength[columnIndex]) {
            return -1;
        }
        System.arraycopy(this.internalBuffer.array(), this.columnOffset[columnIndex], buffer, offset, this.columnLength[columnIndex]);
        return this.columnLength[columnIndex];
    }
    
    public byte[] getRowData() {
        return this.rowData;
    }
    
    public void setRowData(final byte[] rowData) {
        if (rowData != null) {
            this.rowData = Arrays.copyOf(rowData, rowData.length);
        }
    }
    
    @Override
    public void update(final Observable o, final Object arg) {
        super.update(o, arg);
        switch ((com.datasphere.source.lib.constant.Constant.eventType)arg) {
            case ON_OPEN: {
                this.setRecordCount(0);
                this.recordBeginOffset = 0L;
                this.recordEndOffset = 0L;
                if (this.logger.isInfoEnabled()) {
                    this.logger.info((Object)(((ReaderBase)o).name() + " is opened "));
                }
                this.recordCheckpoint.seekPosition(0L);
                this.recordCheckpoint.setSourceName(this.reader().name());
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
}
