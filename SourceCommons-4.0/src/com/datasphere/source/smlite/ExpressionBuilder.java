package com.datasphere.source.smlite;

import org.apache.log4j.*;

public class ExpressionBuilder
{
    public static final String DEFAULT_EXPRESSION = "Expression";
    public static final String NUMERIC_EXPRESSION = "NumericExpression";
    static Logger logger;
    
    static synchronized Expression buildExpression(final CharParser parser, final String delimiter, SMEvent eventObject) {
        final Pattern pattern = PatternFactory.createPattern(delimiter);
        eventObject.length = delimiter.length();
        eventObject = pattern.updateEventAttribute(eventObject);
        final String[] delList = pattern.getListOfPatterns();
        if (pattern.expressionType.equals("NumericExpression")) {
            return new NumericExpression(parser, delList, eventObject);
        }
        if (ExpressionBuilder.logger.isTraceEnabled()) {
            ExpressionBuilder.logger.trace((Object)("Using default expression for {" + delimiter + "}"));
        }
        return new Expression(parser, delList, eventObject);
    }
    
    static {
        ExpressionBuilder.logger = Logger.getLogger((Class)ExpressionBuilder.class);
    }
}
