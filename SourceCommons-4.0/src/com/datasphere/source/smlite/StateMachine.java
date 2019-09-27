package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;
import com.datasphere.source.lib.reader.*;

import java.nio.*;
import org.apache.log4j.*;
import com.datasphere.common.exc.*;

public class StateMachine
{
    protected SMProperty prop;
    protected SMCallback callback;
    protected CharParser parser;
    protected Reader dataSource;
    boolean canBreak;
    boolean canFlushBuffer;
    boolean hasRecordBegin;
    boolean supportsMutipleEndpoint;
    protected char[] bufferArray;
    int bufferOffset;
    int bufferLimit;
    int blockSize;
    boolean inQuote;
    SMEvent[] eventStack;
    RowEvent rEvent;
    RowEvent endOfBlock;
    ResetEvent resetEvent;
    int maxDelimiterLength;
    boolean stopCalled;
    int lastRecordEndPosition;
    CharBuffer internalBuffer;
    Logger logger;
    boolean duplicateEndOfBlockEvent;
    boolean raiseEndOfBlockEvent;
    int trashDataLen;
    RecordException noRecordException;
    
    public StateMachine(final Reader reader, final SMProperty _prop) {
        this.stopCalled = false;
        this.logger = Logger.getLogger((Class)StateMachine.class);
        this.trashDataLen = 0;
        this.noRecordException = null;
        this.dataSource = reader;
        this.prop = _prop;
    }
    
    protected void init() {
        this.bufferOffset = 0;
        this.bufferLimit = 0;
        this.internalBuffer = CharBuffer.allocate(this.dataSource.blockSize() * 2);
        this.bufferArray = this.internalBuffer.array();
        this.lastRecordEndPosition = 0;
        this.blockSize = this.dataSource.blockSize() * 2;
        this.supportsMutipleEndpoint = this.dataSource.supportsMutipleEndpoint();
        this.duplicateEndOfBlockEvent = false;
        this.resetEvent = new ResetEvent();
        this.initializeStateObjs();
    }
    
    private void initializeStateObjs() {
        this.eventStack = new SMEvent[13];
        this.rEvent = new RowEvent(this.bufferArray);
        this.eventStack[2] = this.rEvent;
        this.eventStack[1] = EventFactory.createEvent((short)1);
        this.eventStack[3] = EventFactory.createEvent((short)3);
        this.eventStack[4] = EventFactory.createEvent((short)4);
        this.eventStack[5] = EventFactory.createEvent((short)5);
        this.eventStack[0] = EventFactory.createEvent((short)0);
        this.eventStack[10] = EventFactory.createEvent((short)10);
        this.eventStack[8] = EventFactory.createEvent((short)8);
        this.eventStack[6] = EventFactory.createEvent((short)6);
        if (this.prop.blockAsCompleteRecord) {
            (this.endOfBlock = new RowEvent(this.bufferArray)).length(0);
            this.eventStack[7] = this.endOfBlock;
        }
        final SMEvent nvpEvent = new NVPEvent(this.bufferArray);
        this.eventStack[12] = nvpEvent;
    }
    
    public StateMachine(final SMProperty _prop) {
        this.stopCalled = false;
        this.logger = Logger.getLogger((Class)StateMachine.class);
        this.trashDataLen = 0;
        this.noRecordException = null;
        this.prop = _prop;
    }
    
    public StateMachine(final SMProperty _prop, final SMCallback _callback) {
        this.stopCalled = false;
        this.logger = Logger.getLogger((Class)StateMachine.class);
        this.trashDataLen = 0;
        this.noRecordException = null;
        this.prop = _prop;
        this.callback = _callback;
    }
    
    public void callback(final SMCallback _callback) {
        this.callback = _callback;
    }
    
    public Reader reader() {
        return this.dataSource;
    }
    
    public void reader(final Reader reader) {
        this.dataSource = reader;
    }
    
    public void parser(final CharParser cParser) {
        this.parser = cParser;
        this.hasRecordBegin = this.parser.hasRecordBeginSupport();
        this.maxDelimiterLength = this.parser.maxDelimiterLength();
    }
    
    public void lastRecordEndPosition(final int offset) {
        this.lastRecordEndPosition = offset;
    }
    
    public int lastRecordEndPosition() {
        return this.lastRecordEndPosition;
    }
    
    protected boolean resetInternalBuffer() throws RecordException, AdapterException {
        if (this.prop.blockAsCompleteRecord) {
            if (this.raiseEndOfBlockEvent) {
                this.raiseEndOfBlockEvent = false;
                return true;
            }
            this.bufferLimit = 0;
        }
        else {
            if (this.hasRecordBegin && this.canFlushBuffer && this.lastRecordEndPosition == 0 && this.bufferOffset > 0) {
                if (this.bufferLimit > this.maxDelimiterLength - 1) {
                    this.lastRecordEndPosition = this.bufferOffset - this.maxDelimiterLength - 1;
                }
                this.trashDataLen = this.lastRecordEndPosition;
            }
            if (this.lastRecordEndPosition > 0) {
                final int leftout = this.bufferOffset - this.lastRecordEndPosition;
                this.resetEvent.currentPosition = this.bufferOffset;
                if (leftout > 0) {
                    if (leftout >= this.blockSize) {
                        this.logger.error((Object)("Running out of buffer, data-source: {" + this.dataSource.name() + "}"));
                        throw new AdapterException("Running out of buffer, data-source: {" + this.dataSource.name() + "}");
                    }
                    System.arraycopy(this.bufferArray, this.lastRecordEndPosition, this.bufferArray, 0, leftout);
                    this.bufferLimit = leftout;
                    this.lastRecordEndPosition = 0;
                    this.bufferOffset = 0;
                }
                else {
                    if (this.hasRecordBegin && !this.canFlushBuffer) {
                        this.bufferOffset = 0;
                    }
                    this.bufferLimit = 0;
                }
            }
            else if (this.bufferLimit >= this.blockSize / 2) {
                this.logger.warn((Object)("Record seems to be larger than internal buffer size {" + this.blockSize + "}, going to double it {" + this.blockSize * 2 + "}"));
                this.blockSize *= 2;
                final CharBuffer tmpBuffer = CharBuffer.allocate(this.blockSize);
                System.arraycopy(this.bufferArray = this.internalBuffer.array(), 0, tmpBuffer.array(), 0, this.bufferLimit);
                this.internalBuffer = tmpBuffer;
                this.bufferArray = this.internalBuffer.array();
            }
        }
        final CharBuffer data = (CharBuffer)this.dataSource.readBlock();
        if (data != null) {
            this.raiseEndOfBlockEvent = true;
            System.arraycopy(data.array(), 0, this.bufferArray, this.bufferLimit, data.limit());
            this.bufferLimit += data.limit();
            this.lastRecordEndPosition = 0;
            this.ignoreEvents(null);
            this.resetEvent.currentPosition = this.trashDataLen;
            this.trashDataLen = 0;
            this.resetEvent.buffer = this.bufferArray;
            this.publishEvent(this.resetEvent);
            this.parser.reset();
            this.bufferOffset = 0;
            return false;
        }
        if (this.hasRecordBegin) {
            this.bufferOffset = 0;
        }
        if (this.noRecordException == null) {
            this.noRecordException = new RecordException(RecordException.Type.NO_RECORD);
        }
        throw this.noRecordException;
    }
    
    public char getChar() throws RecordException, AdapterException {
        if (this.bufferOffset >= this.bufferLimit && this.resetInternalBuffer()) {
            return '\0';
        }
        return this.bufferArray[this.bufferOffset++];
    }
    
    public void rewind(final int offset) {
        if (this.bufferOffset - offset < 0) {
            this.logger.error((Object)("Trying to rewind beyond the limit (bufferOffset:" + this.bufferOffset + " offset:" + offset + ")"));
            this.bufferOffset = 0;
        }
        else {
            this.bufferOffset -= offset;
        }
    }
    
    public void next() throws RecordException, AdapterException {
        synchronized (this.parser) {
            if (!this.stopCalled) {
                this.parser.next();
            }
        }
    }
    
    public void publishEvent(final short s) {
        final SMEvent eventData = this.eventStack[s];
        if (eventData != null) {
            eventData.position(this.bufferOffset);
            if (eventData.state() == 2) {
                ((RowEvent)this.eventStack[s]).rowBegin(this.lastRecordEndPosition);
                this.lastRecordEndPosition = eventData.position();
            }
            eventData.publishEvent(this.callback);
        }
    }
    
    public void publishEvent(final ResetEvent event) {
        event.publishEvent(this.callback);
    }
    
    public void publishEvent(final RowEvent event) {
        event.rowBegin(this.lastRecordEndPosition);
        this.lastRecordEndPosition = event.position();
        event.buffer = this.bufferArray;
        this.publishEvent((SMEvent)event);
    }
    
    public void publishEvent(final SMEvent event) {
        event.position(this.bufferOffset);
        if (event.state() == 2) {
            ((RowEvent)event).rowBegin(this.lastRecordEndPosition);
            ((RowEvent)event).buffer = this.bufferArray;
            this.lastRecordEndPosition = event.position();
        }
        event.publishEvent(this.callback);
    }
    
    public void ignoreEvents(final short[] event) {
        this.parser.ignoreEvents(event);
    }
    
    public void ignoreEvents(final short[] events, final SMEvent excludeEvent) {
        this.parser.ignoreEvents(events, excludeEvent);
    }
    
    public void reset(final boolean clearBuffer) {
        this.ignoreEvents(null);
        if (clearBuffer) {
            this.internalBuffer.clear();
            this.internalBuffer.flip();
            this.lastRecordEndPosition = 0;
            this.bufferOffset = 0;
            this.bufferLimit = 0;
        }
    }
    
    public CharParser createParser(final SMProperty smProperty) throws AdapterException {
        return new CharParserLite(this, smProperty);
    }
    
    public static boolean canHandle(final com.datasphere.source.sm.SMProperty smProperty) {
        return true;
    }
    
    protected boolean validateProperty() {
        return true;
    }
    
    public char[] buffer() {
        return this.bufferArray;
    }
    
    public boolean canBreak() {
        return this.canBreak;
    }
    
    public void canBreak(final boolean org) {
        this.canBreak = org;
    }
    
    public void canFlush(final boolean flush) {
        this.canFlushBuffer = flush;
    }
    
    public void close() {
        synchronized (this.parser) {
            this.stopCalled = true;
            this.parser.close();
            if (this.internalBuffer != null) {
                this.internalBuffer.clear();
            }
            this.internalBuffer = null;
            this.eventStack = null;
            this.bufferArray = null;
            if (this.rEvent != null) {
                this.rEvent.buffer = null;
            }
            if (this.resetEvent != null) {
                this.resetEvent.buffer = null;
            }
            this.rEvent = null;
        }
    }
}
