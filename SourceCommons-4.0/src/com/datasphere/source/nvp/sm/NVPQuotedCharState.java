package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class NVPQuotedCharState extends NVPState
{
    NVPStateMachine stateMachine;
    Property property;
    boolean isCharAcceptanaceCheck;
    
    NVPQuotedCharState(final NVPStateMachine context, final Property prop) {
        this.stateMachine = context;
        this.property = prop;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        if (inputChar == this.property.quotecharacter) {
            this.stateMachine.setCurrentState(this.stateMachine.getStartState());
            return Constant.status.NORMAL;
        }
        if (this.isCharAcceptanaceCheck) {
            this.isCharAcceptanaceCheck = false;
            return Constant.status.NOT_ACCEPTED;
        }
        return Constant.status.NORMAL;
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCharAcceptanaceCheck = true;
        return this.process(inputChar);
    }
}
