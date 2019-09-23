package com.datasphere.runtime;

import org.apache.log4j.*;
import java.util.*;

public class ExceptionControlAction
{
    private static final Logger logger;
    private String name;
    private ActionType actionType;
    private List<Property> props;
    
    public ExceptionControlAction() {
        this("exceptionAction", ActionType.CONTINUE, null);
    }
    
    public ExceptionControlAction(final String name, final ActionType type, final List<Property> props) {
        this.props = null;
        this.name = name;
        this.actionType = type;
        this.props = props;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public ActionType getActionType() {
        return this.actionType;
    }
    
    public void setActionType(final ActionType actionType) {
        this.actionType = actionType;
    }
    
    public List<Property> getProps() {
        return this.props;
    }
    
    public void setProps(final List<Property> props) {
        this.props = props;
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        logger = Logger.getLogger((Class)ExceptionControlAction.class);
    }
}
