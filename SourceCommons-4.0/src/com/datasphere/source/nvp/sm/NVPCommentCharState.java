package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.nvp.*;

public class NVPCommentCharState extends NVPState
{
    NVPStateMachine stateMachine;
    NVPProperty property;
    boolean isCharAcceptanaceCheck;
    
    NVPCommentCharState(final NVPStateMachine stateMachine, final NVPProperty property) {
        this.stateMachine = null;
        this.property = null;
        this.stateMachine = stateMachine;
        this.property = property;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        if (inputChar == this.property.commentcharacter) {
            this.stateMachine.previousState = this;
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            return Constant.status.IN_COMMENT;
        }
        if (this.isCharAcceptanaceCheck) {
            this.isCharAcceptanaceCheck = false;
            return Constant.status.NOT_ACCEPTED;
        }
        this.stateMachine.setCurrentState(this.stateMachine.getStartState());
        return Constant.status.NORMAL;
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCharAcceptanaceCheck = true;
        return this.process(inputChar);
    }
}
