package com.datasphere.source.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class SpecialState extends StartState
{
    StateMachine stateMachine;
    Property property;
    boolean isCharAcceptanaceCheck;
    
    public SpecialState(final StateMachine context, final Property prop) {
        super(context, prop);
        this.stateMachine = context;
        this.property = prop;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        final Constant.status s = super.process(inputChar);
        if (s != Constant.status.NORMAL) {
            return s;
        }
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
    
    @Override
    public void reset() {
    }
}
