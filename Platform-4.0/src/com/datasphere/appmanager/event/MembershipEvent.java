package com.datasphere.appmanager.event;

import com.datasphere.uuid.*;

public class MembershipEvent extends Event
{
    private final UUID member;
    private final EventAction eventAction;
    
    public MembershipEvent(final EventAction action, final UUID member) {
        super(null);
        this.member = member;
        this.eventAction = action;
    }
    
    public UUID getMember() {
        return this.member;
    }
    
    @Override
    public EventAction getEventAction() {
        return this.eventAction;
    }
    
    @Override
    public String toString() {
        return " Member: " + this.member + " EventAction: " + this.eventAction;
    }
}
