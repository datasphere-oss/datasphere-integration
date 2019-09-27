package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class NVPDelimiterProcessingState extends NVPState
{
    protected String delimiter;
    protected int delimiterOffset;
    NVPStateMachine stateMachine;
    boolean isCanAcceptanaceCheck;
    boolean delayEventPublish;
    int occurance;
    byte[] tmp;
    Delimiter[] delimiterList;
    Delimiter matchedDelimiter;
    
    public NVPDelimiterProcessingState(final NVPStateMachine stateMachine, final Property prop) {
        this.occurance = 0;
        this.stateMachine = stateMachine;
        this.delayEventPublish = false;
        this.occurance = 0;
        final String[] tmpDel = prop.nvpvaluedelimiter;
        this.delimiterList = new Delimiter[tmpDel.length];
        for (int idx = 0; idx < tmpDel.length; ++idx) {
            this.delimiterList[idx] = new Delimiter(tmpDel[idx]);
        }
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        boolean delimiterFound = false;
        boolean doneWithValidation = false;
        boolean escapeThisDelimiter = false;
        for (int idx = 0; idx < this.delimiterList.length; ++idx) {
            if (this.delimiterList[idx].compare(inputChar) && this.delimiterList[idx].isCompleted()) {
                doneWithValidation = true;
                if (this.delimiterList[idx].isMatched()) {
                    delimiterFound = true;
                    if (this.isEscapeSequence(idx)) {
                        escapeThisDelimiter = true;
                    }
                    this.matchedDelimiter = this.delimiterList[idx];
                    break;
                }
            }
        }
        for (int idx = 0; idx < this.delimiterList.length; ++idx) {
            this.delimiterList[idx].reset(false);
        }
        if (doneWithValidation) {
            if (delimiterFound) {
                this.stateMachine.setCurrentState(this.stateMachine.getSpecialState());
                for (int idx = 0; idx < this.delimiterList.length; ++idx) {
                    this.delimiterList[idx].reset(true);
                }
                if (escapeThisDelimiter) {
                    this.delayEventPublish = false;
                    return Constant.status.NORMAL;
                }
                this.delayEventPublish = true;
                return Constant.status.NORMAL;
            }
            else {
                escapeThisDelimiter = false;
                this.matchedDelimiter = null;
                if (this.isCanAcceptanaceCheck) {
                    this.isCanAcceptanaceCheck = false;
                    return Constant.status.NOT_ACCEPTED;
                }
                this.stateMachine.setCurrentState(this.stateMachine.previousState);
                return Constant.status.NORMAL;
            }
        }
        else {
            this.matchedDelimiter = null;
            if (this.isCanAcceptanaceCheck) {
                this.isCanAcceptanaceCheck = false;
                return Constant.status.NOT_ACCEPTED;
            }
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            return Constant.status.NORMAL;
        }
    }
    
    public boolean isEscapeSequence(final int index) {
        return this.delimiterList[index] == this.matchedDelimiter;
    }
    
    public Constant.status getStatusToPublish() {
        return Constant.status.END_OF_COLUMN;
    }
    
    public boolean hasEventTobePublished() {
        if (this.delayEventPublish) {
            this.delayEventPublish = false;
            return true;
        }
        return false;
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCanAcceptanaceCheck = true;
        return this.process(inputChar);
    }
    
    public int getMatchedDelimiterLength() {
        return this.delimiter.length();
    }
}
