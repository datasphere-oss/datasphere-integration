package com.datasphere.source.smlite;

import org.apache.log4j.*;

public class Pattern implements Cloneable
{
    String expressionType;
    String pattern;
    private static final Logger logger;
    
    public Pattern() {
        this.expressionType = "";
    }
    
    public void init(final String pattern) {
        this.pattern = pattern;
        this.expressionType = "Expression";
    }
    
    public SMEvent updateEventAttribute(final SMEvent event) {
        return event;
    }
    
    public String[] getListOfPatterns() {
        return new String[] { this.pattern };
    }
    
    public Object clone() {
        Object clone = null;
        try {
            clone = super.clone();
        }
        catch (CloneNotSupportedException e) {
            Pattern.logger.warn((Object)"CloneNotSupportedException got while cloning Pattern object ", (Throwable)e);
        }
        return clone;
    }
    
    static {
        logger = Logger.getLogger((Class)Pattern.class);
    }
}
