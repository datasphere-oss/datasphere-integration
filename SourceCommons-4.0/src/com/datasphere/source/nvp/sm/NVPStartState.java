package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class NVPStartState extends NVPState
{
    NVPStateMachine stateMachine;
    Property property;
    int colOffset;
    
    NVPStartState(final NVPStateMachine context, final Property prop) {
        this.stateMachine = context;
        this.property = prop;
        this.colOffset = 0;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        this.setCurrentState();
        final NVPState[] stack = this.stateMachine.getStateStack();
        for (int index = 0; index < stack.length; ++index) {
            final Constant.status returnStatus = stack[index].canAccept(inputChar);
            if (returnStatus != Constant.status.NOT_ACCEPTED) {
                return returnStatus;
            }
        }
        return this.publishDelayedEvent();
    }
    
    public Constant.status publishDelayedEvent() {
        return Constant.status.NORMAL;
    }
    
    public void setCurrentState() {
        if (this.stateMachine.currentState != this) {
            this.stateMachine.setCurrentState(this);
        }
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        return null;
    }
}
