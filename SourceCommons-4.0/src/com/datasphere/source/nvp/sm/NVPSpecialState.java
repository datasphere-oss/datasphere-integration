package com.datasphere.source.nvp.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class NVPSpecialState extends NVPStartState
{
    NVPStateMachine stateMachine;
    Property property;
    boolean isCharAcceptanaceCheck;
    
    NVPSpecialState(final NVPStateMachine context, final Property prop) {
        super(context, prop);
        this.stateMachine = context;
        this.property = prop;
    }
    
    @Override
    public Constant.status publishDelayedEvent() {
        this.stateMachine.setCurrentState(this.stateMachine.getStartState());
        if (this.stateMachine.colDelimiterState.hasEventTobePublished()) {
            return this.stateMachine.colDelimiterState.getStatusToPublish();
        }
        if (this.stateMachine.rowDelimiterState.hasEventTobePublished()) {
            return this.stateMachine.rowDelimiterState.getStatusToPublish();
        }
        return Constant.status.NORMAL;
    }
    
    @Override
    public Constant.status canAccept(final char inputChar) {
        this.isCharAcceptanaceCheck = true;
        return this.process(inputChar);
    }
}
