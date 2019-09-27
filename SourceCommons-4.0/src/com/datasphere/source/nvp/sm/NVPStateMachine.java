package com.datasphere.source.nvp.sm;

import java.util.*;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.intf.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.source.nvp.*;

public class NVPStateMachine
{
    NVPState[] stateObjStack;
    public LinkedList<Constant.status> eventQueue;
    NVPStartState startState;
    NVPQuotedCharState quotedCharState;
    NVPSpecialState specialState;
    NVPCommentCharState commentState;
    NVPDelimiterProcessingState colDelimiterState;
    NVPRowDelimiterProcessingState rowDelimiterState;
    NVPState previousState;
    NVPState currentState;
    Constant.status currentStatus;
    Property property;
    int type;
    
    public NVPStateMachine(final NVPProperty prop) {
        this.commentState = null;
        this.type = 1;
        this.startState = new NVPStartState(this, prop);
        this.quotedCharState = new NVPQuotedCharState(this, prop);
        this.specialState = new NVPSpecialState(this, prop);
        this.commentState = new NVPCommentCharState(this, prop);
        this.colDelimiterState = new NVPDelimiterProcessingState(this, prop);
        this.rowDelimiterState = new NVPRowDelimiterProcessingState(this, prop);
        this.eventQueue = new LinkedList<Constant.status>();
        (this.stateObjStack = new NVPState[4])[0] = this.commentState;
        this.stateObjStack[1] = this.quotedCharState;
        this.stateObjStack[2] = this.rowDelimiterState;
        this.stateObjStack[3] = this.colDelimiterState;
        this.currentState = this.startState;
        this.previousState = this.currentState;
        this.property = prop;
    }
    
    public Constant.status process(final char inputChar) {
        return this.currentState.process(inputChar);
    }
    
    public NVPState[] getStateStack() {
        return this.stateObjStack;
    }
    
    public NVPDelimiterProcessingState getDelimiterProcessingState() {
        return this.colDelimiterState;
    }
    
    public NVPRowDelimiterProcessingState getRowDelimiterProcessingState() {
        return this.rowDelimiterState;
    }
    
    public NVPStartState getStartState() {
        return this.startState;
    }
    
    public NVPQuotedCharState getQuotedCharState() {
        return this.quotedCharState;
    }
    
    public NVPCommentCharState getCommentCharState() {
        return this.commentState;
    }
    
    public State getCurrentState() {
        return this.currentState;
    }
    
    public Constant.status getCurrentStatus() {
        return this.currentStatus;
    }
    
    public NVPSpecialState getSpecialState() {
        return this.specialState;
    }
    
    public void setCurrentState(final NVPState currentState) {
        this.currentState = currentState;
    }
    
    public void setCurrentStatus(final Constant.status currentStatus) {
        this.currentStatus = currentStatus;
    }
    
    Constant.status getMode() {
        return this.currentStatus;
    }
    
    public void reset() {
        this.type = 1;
        this.setCurrentState(this.getStartState());
    }
}
