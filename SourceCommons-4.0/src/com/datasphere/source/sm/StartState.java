package com.datasphere.source.sm;

import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.prop.*;

public class StartState extends SMState
{
    StateMachine stateMachine;
    Property property;
    int colOffset;
    char[] coiArray;
    
    public StartState(final StateMachine context, final Property prop) {
        this.stateMachine = context;
        this.property = prop;
        this.colOffset = 0;
        this.coiArray = this.getCharsOfInterest().toCharArray();
    }
    
    @Override
    public String getCharsOfInterest() {
        String ret = "";
        for (int idx = 0; idx < this.stateMachine.getStateStack().length; ++idx) {
            final String coi = this.stateMachine.getStateStack()[idx].getCharsOfInterest();
            ret += coi;
        }
        return ret;
    }
    
    @Override
    public Constant.status process(final char inputChar) {
        this.stateMachine.setCurrentState(this);
        boolean foundInteresting = false;
        for (final char c : this.coiArray) {
            if (c == inputChar) {
                foundInteresting = true;
                break;
            }
        }
        if (foundInteresting) {
            final SMState[] stack = this.stateMachine.getStateStack();
            for (int index = 0; index < stack.length; ++index) {
                final Constant.status returnStatus = stack[index].canAccept(inputChar);
                if (returnStatus != Constant.status.NOT_ACCEPTED) {
                    return returnStatus;
                }
            }
        }
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
    
    @Override
    public void reset() {
        this.setCurrentState();
    }
}
