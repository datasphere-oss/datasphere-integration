package com.datasphere.hd;

import com.datasphere.runtime.exceptions.*;

import java.util.*;

import com.datasphere.exception.*;
import com.datasphere.persistence.*;
import java.io.*;

public class HDApiRestCompiler
{
    StringBuilder selectString;
    
    public HDApiRestCompiler() {
        this.selectString = new StringBuilder();
    }
    
    public String createSelectString(final String dataStore, final String[] fields, final Map<String, Object> filters, final String keyField, final String[] implicitFields, final boolean getEventList) throws NoOperatorFoundException, InvalidFormatException {
        this.selectString.setLength(0);
        this.selectString.append("SELECT ");
        if ((fields == null && !getEventList) || (fields.length == 0 && !getEventList)) {
            throw new InvalidFormatException("SELECT expression need atleast 1 field or event list");
        }
        if (fields != null && fields.length > 0) {
            for (int ii = 0; ii < fields.length; ++ii) {
                if (ii != fields.length - 1) {
                    this.selectString.append(fields[ii]);
                    this.selectString.append(", ");
                }
                else {
                    this.selectString.append(fields[ii]);
                }
            }
        }
        if (getEventList) {
            this.selectString.append(", ");
            this.selectString.append("eventList(w)");
        }
        this.selectString.append(" ");
        this.selectString.append("FROM ");
        this.selectString.append(dataStore);
        if (getEventList) {
            this.selectString.append(" ");
            this.selectString.append("w");
        }
        this.selectString.append(" ");
        if (filters != null) {
            this.addFilters(filters, null, null);
        }
        this.selectString.append(";");
        return this.selectString.toString();
    }
    
    public void addFilters(final Map<String, Object> filters, final String groupByVal, final String orderByVal) {
        final Map<String, Object> ctxFilter = (Map<String, Object>)filters.get("context");
        if (ctxFilter != null && !ctxFilter.isEmpty()) {
            this.selectString.append("WHERE ");
            int totalConditions = ctxFilter.size();
            for (final Map.Entry<String, Object> conditions : ctxFilter.entrySet()) {
                final String ctxField = conditions.getKey();
                final String ctxCondition = (String)conditions.getValue();
                final OperatorOnField operatorOnField = this.stripOperator(ctxCondition);
                switch (operatorOnField.operator) {
                    case IN: {
                        final String[] inValues = operatorOnField.data.split("[~]");
                        this.selectString.append(ctxField + " " + operatorOnField.operator.name() + "(");
                        for (int ii = 0; ii < inValues.length; ++ii) {
                            if (ii != inValues.length - 1) {
                                this.selectString.append(inValues[ii]);
                                this.selectString.append(", ");
                            }
                            else {
                                this.selectString.append(inValues[ii]);
                            }
                        }
                        this.selectString.append(")");
                        break;
                    }
                    case BTWN: {
                        final String[] btwnValues = operatorOnField.data.split("[~]");
                        if (btwnValues.length != 2) {
                            throw new InvalidFormatException(operatorOnField.operator.name() + " expects 2 values");
                        }
                        this.selectString.append(ctxField);
                        this.selectString.append(" ");
                        this.selectString.append("BETWEEN ");
                        this.selectString.append(btwnValues[0]);
                        this.selectString.append(" ");
                        this.selectString.append(" AND ");
                        this.selectString.append(" ");
                        this.selectString.append(btwnValues[1]);
                        break;
                    }
                    case LIKE: {
                        this.selectString.append(ctxField);
                        this.selectString.append(" ");
                        this.selectString.append(operatorOnField.operator.name());
                        this.selectString.append("'%");
                        this.selectString.append(operatorOnField.data);
                        this.selectString.append("%'");
                        break;
                    }
                    case GT: {
                        this.selectString.append(ctxField);
                        this.selectString.append(">");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                    case LT: {
                        this.selectString.append(ctxField);
                        this.selectString.append("<");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                    case GTE: {
                        this.selectString.append(ctxField);
                        this.selectString.append(">=");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                    case LTE: {
                        this.selectString.append(ctxField);
                        this.selectString.append("<=");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                    case EQ: {
                        this.selectString.append(ctxField);
                        this.selectString.append("=");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                    case NE: {
                        this.selectString.append(ctxField);
                        this.selectString.append("<>");
                        this.selectString.append(operatorOnField.data);
                        break;
                    }
                }
                if (--totalConditions != 0) {
                    this.selectString.append(" AND ");
                }
            }
        }
        if (filters.containsKey("starttime")) {
            if (filters.containsKey("endtime")) {
                this.selectString.append(" AND ");
                this.selectString.append("$timestamp");
                this.selectString.append(" ");
                this.selectString.append("BETWEEN ");
                this.selectString.append(filters.get("starttime"));
                this.selectString.append(" AND ");
                this.selectString.append(filters.get("endtime"));
            }
            else {
                this.selectString.append(" AND ");
                this.selectString.append("$timestamp");
                this.selectString.append(">");
                this.selectString.append(filters.get("starttime"));
            }
        }
        else if (filters.containsKey("endtime")) {
            this.selectString.append(" AND ");
            this.selectString.append("$timestamp");
            this.selectString.append("<");
            this.selectString.append(filters.get("endtime"));
        }
        if (filters.containsKey("groupby")) {
            this.selectString.append(" ");
            this.selectString.append("GROUP BY ");
            if (groupByVal != null) {
                this.selectString.append(groupByVal);
            }
            else {
                final String gBy = (String)filters.get("groupby");
                final String[] gByArray = gBy.split("~");
                for (int ii2 = 0; ii2 < gByArray.length; ++ii2) {
                    if (ii2 != gByArray.length - 1) {
                        this.selectString.append(gByArray[ii2]);
                        this.selectString.append(", ");
                    }
                    else {
                        this.selectString.append(gByArray[ii2]);
                    }
                }
            }
        }
        if (filters.containsKey("sortby")) {
            this.selectString.append(" ");
            this.selectString.append("ORDER BY ");
            final String oBy = (String)filters.get("sortby");
            final String[] oByArray = oBy.split("~");
            for (int ii2 = 0; ii2 < oByArray.length; ++ii2) {
                if (ii2 != oByArray.length - 1) {
                    this.selectString.append(oByArray[ii2]);
                    this.selectString.append(", ");
                }
                else {
                    this.selectString.append(oByArray[ii2]);
                }
            }
            if (orderByVal != null) {
                this.selectString.append(", ");
                this.selectString.append(orderByVal);
            }
            if (filters.containsKey("sortdir")) {
                this.selectString.append(" ");
                this.selectString.append(filters.get("sortdir"));
            }
        }
        if (filters.containsKey("limit")) {
            this.selectString.append(" ");
            this.selectString.append("LIMIT ");
            this.selectString.append(filters.get("limit"));
        }
    }
    
    public String createQueryToGetLatestPerKey(final String dataStore, final String[] fields, final Map<String, Object> filters, final String keyField, final String[] implicitFields, final boolean getEventList) {
        if ((fields == null && !getEventList) || (fields.length == 0 && !getEventList)) {
            throw new InvalidFormatException("SELECT expression need atleast 1 field or event list");
        }
        this.selectString.setLength(0);
        this.selectString.append("SELECT ");
        if (fields != null && fields.length > 0) {
            for (int ii = 0; ii < fields.length; ++ii) {
                if (ii != fields.length - 1) {
                    (this.selectString = this.addField(fields[ii])).append(", ");
                }
                else {
                    this.selectString = this.addField(fields[ii]);
                }
            }
        }
        if (getEventList) {
            this.selectString.append(", ");
            this.selectString.append("eventList(w)");
        }
        this.selectString.append(" ");
        this.selectString.append("FROM ");
        this.selectString.append(dataStore);
        this.selectString.append(" ");
        this.selectString.append("w");
        this.selectString.append(" ");
        if (filters.containsKey("singlehds")) {
            final Object removed = filters.remove("singlehds");
            filters.put("groupby", removed);
        }
        this.addFilters(filters, keyField, "$timestamp");
        this.selectString.append(";");
        return this.selectString.toString();
    }
    
    public StringBuilder addField(final String field) {
        this.selectString.append("last");
        this.selectString.append("(");
        this.selectString.append("w." + field);
        this.selectString.append(")");
        return this.selectString;
    }
    
    public OperatorOnField stripOperator(final String ctxCondition) throws NoOperatorFoundException, InvalidFormatException {
        final String[] conditionSplit = ctxCondition.split("\\$");
        if (conditionSplit.length != 3) {
            throw new InvalidFormatException("Format is $OPERATOR$FIELD");
        }
        if (conditionSplit[1] == null || conditionSplit[1].isEmpty()) {
            throw new NoOperatorFoundException("No Operator defined for to run a Query.");
        }
        if (conditionSplit[2] != null && !conditionSplit[2].isEmpty()) {
            final HQuery.OpType opType = HQuery.OpType.valueOf(conditionSplit[1]);
            final OperatorOnField operatorOnField = new OperatorOnField(opType, conditionSplit[2]);
            return operatorOnField;
        }
        throw new InvalidFormatException("Can't apply operator " + conditionSplit[1] + " on Null or Empty Data Set");
    }
    
    public class OperatorOnField
    {
        public HQuery.OpType operator;
        public String data;
        
        public OperatorOnField(final HQuery.OpType operator, final String field) {
            this.operator = operator;
            this.data = field;
        }
    }
}
