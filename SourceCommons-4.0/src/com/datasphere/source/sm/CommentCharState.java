package com.datasphere.source.sm;

import com.datasphere.source.lib.constant.*;

public class CommentCharState extends SMState
{
    StateMachine stateMachine;
    SMProperty property;
    boolean isCharAcceptanaceCheck;
    boolean inCommentMode;
    
    CommentCharState(final StateMachine stateMachine, final SMProperty property) {
        this.stateMachine = null;
        this.property = null;
        this.stateMachine = stateMachine;
        this.property = property;
    }
    
    @Override
    public String getCharsOfInterest() {
        return "" + this.property.commentcharacter;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        if (inputChar == this.property.commentcharacter) {
            this.stateMachine.previousState = this;
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            this.inCommentMode = true;
            return Constant.status.IN_COMMENT;
        }
        if (!this.isCharAcceptanaceCheck) {
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            return Constant.status.NORMAL;
        }
        if (!this.inCommentMode) {
            this.isCharAcceptanaceCheck = false;
            return Constant.status.NOT_ACCEPTED;
        }
        final Constant.status st = this.stateMachine.rowDelimiterState.canAccept(inputChar);
        if (st == Constant.status.END_OF_ROW) {
            this.inCommentMode = false;
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            return Constant.status.END_OF_COMMENT;
        }
        return Constant.status.IN_COMMENT;
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCharAcceptanaceCheck = true;
        return this.process(inputChar);
    }
    
    @Override
    public void reset() {
        this.inCommentMode = false;
        this.isCharAcceptanaceCheck = false;
        this.stateMachine.previousState = this.stateMachine.startState;
        this.stateMachine.setCurrentState(this.stateMachine.startState);
    }
}
