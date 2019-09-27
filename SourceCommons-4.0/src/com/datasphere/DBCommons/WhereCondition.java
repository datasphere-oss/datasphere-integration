package com.datasphere.DBCommons;

import org.apache.log4j.*;
import java.util.*;
import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;

class WhereCondition implements IExpressionVisitor
{
    Logger logger;
    private TExpression condition;
    HashMap<String, String> whereColumnsValues;
    
    public WhereCondition(final TExpression expr) {
        this.logger = Logger.getLogger((Class)CommonSQLParser.class);
        this.condition = expr;
        this.whereColumnsValues = new HashMap<String, String>();
    }
    
    public HashMap<String, String> parseColumns() {
        this.condition.inOrderTraverse((IExpressionVisitor)this);
        return this.whereColumnsValues;
    }
    
    boolean is_compare_condition(final EExpressionType t) {
        return t == EExpressionType.simple_comparison_t || t == EExpressionType.group_comparison_t || t == EExpressionType.in_t;
    }
    
    public boolean exprVisit(final TParseTreeNode pnode, final boolean pIsLeafNode) {
        final TExpression lcexpr = (TExpression)pnode;
        if (this.is_compare_condition(lcexpr.getExpressionType())) {
            final TExpression leftExpr = lcexpr.getLeftOperand();
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Column Name: " + leftExpr.toString()));
            }
            if (lcexpr.getComparisonOperator() != null && this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Operator: " + lcexpr.getComparisonOperator().astext));
            }
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Column Value: " + lcexpr.getRightOperand().toString()));
            }
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Column Name: " + leftExpr.toString() + "Column Value: " + lcexpr.getRightOperand().toString()));
            }
            this.whereColumnsValues.put(leftExpr.toString().substring(1, leftExpr.toString().length() - 1), lcexpr.getRightOperand().toString().substring(1, lcexpr.getRightOperand().toString().length() - 1));
        }
        return true;
    }
}
