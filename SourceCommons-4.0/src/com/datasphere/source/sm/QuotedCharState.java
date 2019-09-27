package com.datasphere.source.sm;

import com.datasphere.source.lib.constant.*;

public class QuotedCharState extends SMState
{
    StateMachine stateMachine;
    SMProperty property;
    boolean isCharAcceptanaceCheck;
    boolean inQuoteBlock;
    QuoteSet[] quoteList;
    QuoteSet matchedQuoteSet;
    QuoteSet lastMatchedQuoteSet;
    boolean ignoreRowDelimiterInQuote;
    char[] coiArray;
    
    public QuotedCharState(final StateMachine context, final SMProperty prop) {
        this.stateMachine = context;
        this.property = prop;
        this.quoteList = null;
        this.matchedQuoteSet = null;
        this.lastMatchedQuoteSet = null;
        this.ignoreRowDelimiterInQuote = prop.ignoreRowDelimiterInQueue;
        final String[] qList = this.property.quoteSetList;
        if (qList.length > 0) {
            this.quoteList = new QuoteSet[qList.length];
            for (int idx = 0; idx < qList.length; ++idx) {
                this.quoteList[idx] = new QuoteSet(qList[idx]);
            }
        }
        this.coiArray = this.getCharsOfInterest().toCharArray();
    }
    
    @Override
    public String getCharsOfInterest() {
        String ret = "";
        for (int idx = 0; idx < this.quoteList.length; ++idx) {
            ret += this.quoteList[idx].getQuoteAsString();
        }
        return ret;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        if (this.quoteList != null) {
            if (this.matchedQuoteSet != null) {
                if (!this.ignoreRowDelimiterInQuote) {
                    final Constant.status st = this.stateMachine.getRowDelimiterProcessingState().process(inputChar);
                    this.stateMachine.setCurrentState(this);
                    if (st == Constant.status.END_OF_ROW) {
                        this.reset();
                        return st;
                    }
                }
                final QuoteSet.QuoteState state = this.matchedQuoteSet.match(inputChar);
                if (state == QuoteSet.QuoteState.END) {
                    this.lastMatchedQuoteSet = this.matchedQuoteSet;
                    this.matchedQuoteSet = null;
                    if (this.inQuoteBlock) {
                        this.inQuoteBlock = false;
                        this.stateMachine.setCurrentState(this.stateMachine.startState);
                    }
                    else {
                        System.out.println("Something wrong in our processing");
                    }
                }
            }
            else {
                boolean foundInteresting = false;
                for (final char c : this.coiArray) {
                    if (c == inputChar) {
                        foundInteresting = true;
                        break;
                    }
                }
                if (foundInteresting) {
                    for (int idx = 0; idx < this.quoteList.length; ++idx) {
                        final QuoteSet.QuoteState state2 = this.quoteList[idx].match(inputChar);
                        if (state2 != QuoteSet.QuoteState.NOT_MATCHED) {
                            if (state2 == QuoteSet.QuoteState.BEGIN) {
                                this.matchedQuoteSet = this.quoteList[idx];
                                this.inQuoteBlock = true;
                                this.stateMachine.setCurrentState(this);
                                break;
                            }
                        }
                    }
                }
            }
            if (!this.isCharAcceptanaceCheck) {
                return Constant.status.NORMAL;
            }
            this.isCharAcceptanaceCheck = false;
            if (!this.inQuoteBlock) {
                return Constant.status.NOT_ACCEPTED;
            }
            if (this.stateMachine.colDelimiterState.hasEventTobePublished()) {
                return this.stateMachine.colDelimiterState.getStatusToPublish();
            }
            if (this.stateMachine.rowDelimiterState.hasEventTobePublished()) {
                return this.stateMachine.rowDelimiterState.getStatusToPublish();
            }
            return Constant.status.NORMAL;
        }
        else {
            if (this.isCharAcceptanaceCheck) {
                this.isCharAcceptanaceCheck = false;
                return Constant.status.NOT_ACCEPTED;
            }
            return Constant.status.NORMAL;
        }
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCharAcceptanaceCheck = true;
        return this.process(inputChar);
    }
    
    @Override
    public void reset() {
        this.matchedQuoteSet = null;
        this.lastMatchedQuoteSet = null;
        this.inQuoteBlock = false;
        this.isCharAcceptanaceCheck = false;
        for (int idx = 0; idx < this.quoteList.length; ++idx) {
            this.quoteList[idx].reset();
        }
    }
}
