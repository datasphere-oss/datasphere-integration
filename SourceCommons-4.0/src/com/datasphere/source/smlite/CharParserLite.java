package com.datasphere.source.smlite;

import org.apache.log4j.*;
import com.datasphere.common.exc.*;

public class CharParserLite extends CharParser
{
    Logger logger;
    public static final int HASH_SIZE = 65536;
    short[] hash;
    short[] tmpHash;
    short[] defaultHash;
    
    public CharParserLite(final StateMachine sm, final SMProperty prop) throws AdapterException {
        super(sm, prop);
        this.logger = Logger.getLogger((Class)CharParser.class);
    }
    
    @Override
    protected void init() {
        this.defaultHash = new short[65536];
        this.hash = this.defaultHash;
        this.delimiterList = new String[13][65536];
        super.init();
    }
    
    @Override
    public void finalizeDelimiterInit() {
        this.delimiterList[1] = this.colDelimiter;
        this.delimiterList[2] = this.rowDelimiter;
    }
    
    @Override
    public void next() throws RecordException, AdapterException {
        int charIndex;
        do {
            charIndex = this.stateMachine.getChar();
            if (this.hash[charIndex] != 0) {
                this.stateMachine.publishEvent(this.hash[charIndex]);
            }
        } while (this.hash[charIndex] != 2 && this.hash[charIndex] != 7);
    }
    
    @Override
    public void ignoreEvents(final short[] events) {
        if (events == null) {
            this.hash = this.defaultHash;
        }
        else {
            if (this.tmpHash == null) {
                this.tmpHash = new short[65536];
            }
            System.arraycopy(this.hash, 0, this.tmpHash, 0, this.hash.length);
            this.hash = this.tmpHash;
            for (int eventItr = 0; eventItr < events.length; ++eventItr) {
                final String[] strList = this.delimiterList[events[eventItr]];
                if (strList != null) {
                    for (int idx = 0; idx < strList.length; ++idx) {
                        if (strList[idx] == null) {
                            System.out.println("Break here");
                        }
                        this.hash[strList[idx].charAt(0)] = 0;
                    }
                }
            }
        }
    }
    
    @Override
    public void ignoreEvents(final short[] events, final SMEvent excludeEvent) {
        this.ignoreEvents(events);
        if (excludeEvent != null) {
            final int idx = excludeEvent.delimiter.charAt(0);
            if (this.hash[idx] == 0) {
                this.hash[idx] = excludeEvent.state();
            }
            else if (this.hash[idx] != excludeEvent.state()) {
                this.logger.warn((Object)("Different event type is registered for delimiter {" + excludeEvent.delimiter.charAt(0) + "}"));
            }
        }
    }
    
    @Override
    public boolean hasNext() {
        return false;
    }
    
    public void initializeDelimiter(final String[] delimters, final short eventType) {
        if (delimters != null) {
            for (int itr = 0; itr < delimters.length; ++itr) {
                final int index = delimters[itr].charAt(0);
                if (this.hash[index] != 0 && this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Delimiter [" + delimters[itr] + "] already configured for event [" + this.hash[index] + "] and changed to [" + eventType + "]"));
                }
                this.hash[index] = eventType;
            }
        }
    }
}
