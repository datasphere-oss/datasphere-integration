package com.datasphere.source.smlite;

import org.apache.log4j.*;

public class EventFactory
{
    Logger logger;
    
    public EventFactory() {
        this.logger = Logger.getLogger((Class)EventFactory.class);
    }
    
    public static SMEvent createEvent(final short eventType) {
        switch (eventType) {
            case 2: {
                return new RowEvent();
            }
            case 1: {
                return new ColumnEvent();
            }
            case 0: {
                return new ResetEvent();
            }
            case 3: {
                return new QuoteEvent();
            }
            case 6: {
                return new CommentEvent();
            }
            case 4: {
                return new QuoteBeginEvent();
            }
            case 5: {
                return new QuoteEndEvent();
            }
            case 9: {
                return new TimeStampEvent();
            }
            case 8: {
                return new RowBeginEvent();
            }
            case 7: {
                return new EndOfBlockEvent();
            }
            case 11: {
                return new RowEndEvent();
            }
            case 10: {
                return new EscapeEvent();
            }
            case 12: {
                return new NVPEvent();
            }
            default: {
                return null;
            }
        }
    }
}
