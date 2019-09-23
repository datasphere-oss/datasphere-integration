package com.datasphere.utility;

import org.apache.log4j.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.exceptions.*;
import java.util.*;

public class CQUtility
{
    private static final Logger logger;
    public static final String QUERY_ATTRIBUTE = "attr";
    public static final String QUERY_FEATURE_SELECT = "select";
    public static final String QUERY_SELECT_ALL = "*";
    public static final String QUERY_ATTRIBUTE_ANY = "*any";
    public static final String QUERY_FEATURE_FROM = "from";
    public static final String QUERY_FEATURE_WHERE = "where";
    public static final String QUERY_WHERE_AND = "and";
    public static final String QUERY_WHERE_OR = "or";
    public static final String QUERY_WHERE_NOT = "not";
    public static final String QUERY_WHERE_OPER = "oper";
    public static final String QUERY_WHERE_GT = "gt";
    public static final String QUERY_WHERE_LT = "lt";
    public static final String QUERY_WHERE_GTE = "gte";
    public static final String QUERY_WHERE_LTE = "lte";
    public static final String QUERY_WHERE_EQ = "eq";
    public static final String QUERY_WHERE_NEQ = "neq";
    public static final String QUERY_WHERE_LIKE = "like";
    public static final String QUERY_WHERE_BETWEEN = "between";
    public static final String QUERY_WHERE_IN_SET = "in";
    public static final String QUERY_WHERE_VALUE = "value";
    public static final String QUERY_WHERE_VALUES = "values";
    public static final String QUERY_FEATURE_ORDER_BY = "orderby";
    public static final String QUERY_ORDER_BY_ASCENDING = "ascending";
    public static final String QUERY_FEATURE_GROUP_BY = "groupby";
    public static final String QUERY_FEATURE_HAVING = "having";
    public static final String QUERY_FUNCTION_DISTINCT = "distinct";
    
    public static void main(final String[] args) {
        testQueries("{ \"select\": [ \"sid\", \"sid2\" ] ");
        testQueries("{ \"select\": [ \"sid\", \"sid2\" ] }");
        testQueries("{ \"select\": [ \"sid\", \"sid2\" ],  \"from\":  [ \"xstream\" , \"ywindow\" ] }");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"]}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"gte\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"gt\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"lte\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"lt\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"eq\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"TestColumn\",\"value\":7}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"eq\",\"attr\":\"TestColumn\",\"value\":null}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"TestColumn\",\"value\":null}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"eq\",\"attr\":\"TestColumn\",\"value\":null}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"TestColumn\",\"value\":null}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"and\":[{\"oper\":\"gte\",\"attr\":\"TestColumn\",\"value\":\"Seven\"},{\"oper\":\"lte\",\"attr\":\"TestColumn\",\"value\":\"Three\"}]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"not\":{\"or\":[{\"oper\":\"lt\",\"attr\":\"TestColumn\",\"value\":\"Seven\"},{\"oper\":\"gt\",\"attr\":\"TestColumn\",\"value\":\"Three\"}]}}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"or\":[{\"oper\":\"lt\",\"attr\":\"TestColumn\",\"value\":\"Seven\"},{\"oper\":\"gt\",\"attr\":\"TestColumn\",\"value\":\"Three\"}]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"like\",\"attr\":\"TestColumn\",\"value\":\"Seven\"}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"in\",\"attr\":\"TestColumn\",\"values\":[\"Five\"]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"in\",\"attr\":\"TestColumn\",\"values\":[\"Five\",\"Seven\"]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"in\",\"attr\":\"TestColumn\",\"values\":[\"Eight\"]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"in\",\"attr\":\"TestColumn\",\"values\":[null,\"Five\"]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"between\",\"attr\":\"TestColumn\",\"values\":[\"Seven\",\"Six\"]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"between\",\"attr\":\"TestColumn\",\"values\":[null,\"Five\"]}}");
        testQueries("{\"select\":[{\"oper\":\"count\",\"attr\":\"Column_integer\"}],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null},\"groupby\":[\"Column_string\",\"Column_datetime\"]}");
        testQueries("{\"select\":[{\"oper\":\"count\",\"attr\":\"Column_integer\"},{\"oper\":\"sum\",\"attr\":\"Column_integer\"},{\"oper\":\"min\",\"attr\":\"Column_integer\"},{\"oper\":\"max\",\"attr\":\"Column_integer\"},{\"oper\":\"avg\",\"attr\":\"Column_integer\"}],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null},\"groupby\":[\"Column_string\"]}");
        testQueries("{\"select\":[{\"oper\":\"count\",\"attr\":\"Column_integer\"},{\"oper\":\"sum\",\"attr\":\"Column_integer\"}],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null}}");
        testQueries("{\"select\":[{\"oper\":\"first\",\"attr\":\"Column_integer\"}],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null},\"groupby\":[\"Column_string\"]}");
        testQueries("{\"select\":[{\"oper\":\"last\",\"attr\":\"Column_integer\"}],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null},\"groupby\":[\"Column_string\"]}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"and\":[{\"oper\":\"lte\",\"attr\":\"Column_integer\",\"value\":4},{\"oper\":\"lte\",\"attr\":\"Column_double\",\"value\":4.1},{\"oper\":\"eq\",\"attr\":\"Column_boolean\",\"value\":true}]}}");
        testQueries("{\"select\":[\"*\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"neq\",\"attr\":\"Column_string\",\"value\":null},\"orderby\":[{\"attr\":\"Column_date\"},{\"attr\":\"Column_integer\",\"ascending\":false}]}");
        testQueries("{\"select\":[\"Column_string\",\"Column_integer\",\"Unfetched\"],\"from\":[\"stream1\"],\"where\":{\"oper\":\"eq\",\"attr\":\"Column_string\",\"value\":\"One\"}}");
    }
    
    private static void testQueries(final String query) {
        String result = "";
        try {
            result = convertJSONToSQL(query);
        }
        catch (Exception e) {
            CQUtility.logger.warn((Object)(e.getMessage() + " " + query));
            return;
        }
        CQUtility.logger.warn((Object)"----------");
        CQUtility.logger.warn((Object)query);
        CQUtility.logger.warn((Object)"");
        CQUtility.logger.warn((Object)result);
        CQUtility.logger.warn((Object)"----------\n");
    }
    
    public static String convertJSONToSQL(final String jsonQuery) throws Exception {
        final StringBuilder queryText = new StringBuilder();
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(jsonQuery);
        }
        catch (Exception e) {
            throw new Exception("The JSON is not valid.");
        }
        final List<String> fromClause = checkAndGetFromClause(jsonNode);
        final List<String> selectClause = checkAndGetSelectClause(jsonNode);
        final List<String> groupByClause = checkAndGetGroupByClause(jsonNode);
        final List<String> whereClause = checkAndGetWhereClause(jsonNode);
        final List<String> orderByClause = checkAndGetOrderByClause(jsonNode);
        queryText.append("SELECT ");
        queryText.append(Utility.join(selectClause));
        queryText.append(" FROM ");
        queryText.append(Utility.join(fromClause));
        if (!whereClause.isEmpty()) {
            queryText.append(" WHERE ");
            for (final String string : whereClause) {
                queryText.append(string);
            }
        }
        if (!groupByClause.isEmpty()) {
            queryText.append(" GROUP BY ");
            queryText.append(Utility.join(groupByClause));
        }
        if (!orderByClause.isEmpty()) {
            queryText.append(" ORDER BY ");
            queryText.append(Utility.join(orderByClause));
        }
        return queryText.toString();
    }
    
    private static List<String> checkAndGetOrderByClause(final JsonNode jsonNode) {
        final JsonNode attributes = jsonNode.get("orderby");
        final List<String> orderByResults = new ArrayList<String>();
        if (attributes != null) {
            for (final JsonNode attribute : attributes) {
                final String field = attribute.get("attr").asText();
                final JsonNode ascending = attribute.get("ascending");
                final boolean isAsc = ascending == null || ascending.asBoolean(true);
                if (ascending == null) {
                    orderByResults.add(field);
                }
                else {
                    orderByResults.add(field + " " + (isAsc ? "ASC" : "DESC"));
                }
            }
        }
        return orderByResults;
    }
    
    private static List<String> checkAndGetFromClause(final JsonNode jsonNode) throws Exception {
        final JsonNode jsonFromNode = jsonNode.path("from");
        final int countOfFromFields = jsonFromNode.size();
        if (countOfFromFields == 0) {
            throw new Exception("From clause requires at least one datasource.");
        }
        final List<String> fromFields = new ArrayList<String>(countOfFromFields);
        for (int i = 0; i < countOfFromFields; ++i) {
            fromFields.add(jsonFromNode.get(i).asText());
        }
        return fromFields;
    }
    
    private static List<String> checkAndGetSelectClause(final JsonNode jsonNode) throws Exception {
        final JsonNode jsonFromNode = jsonNode.path("select");
        final int countOfSelectFields = jsonFromNode.size();
        if (countOfSelectFields == 0) {
            throw new Exception("Select clause requires at least one projection.");
        }
        final List<String> selectFields = new ArrayList<String>(countOfSelectFields);
        for (int i = 0; i < countOfSelectFields; ++i) {
            final JsonNode attribute = jsonFromNode.get(i);
            if (attribute.isValueNode()) {
                selectFields.add(attribute.asText());
            }
            else {
                selectFields.add(processProjectionFunction(attribute));
            }
        }
        return selectFields;
    }
    
    private static String processProjectionFunction(final JsonNode function) throws CapabilityException {
        final String functionName = function.get("oper").asText();
        final String attributeName = function.get("attr").asText();
        return functionName.toUpperCase() + "(" + attributeName + ")";
    }
    
    private static List<String> checkAndGetGroupByClause(final JsonNode jsonNode) {
        final JsonNode groupByNodes = jsonNode.get("groupby");
        final List<String> groupByFields = new ArrayList<String>();
        if (groupByNodes != null) {
            for (final JsonNode groupByNode : groupByNodes) {
                groupByFields.add(groupByNode.asText());
            }
        }
        return groupByFields;
    }
    
    private static List<String> checkAndGetWhereClause(final JsonNode jsonNode) {
        final List<String> whereFields = new ArrayList<String>();
        final JsonNode whereClause = jsonNode.get("where");
        if (whereClause != null) {
            whereFields.addAll(getLogicalOperation(whereClause));
        }
        return whereFields;
    }
    
    private static List<String> getLogicalOperation(final JsonNode operation) throws CapabilityException {
        final String s;
        final String operationType = s = operation.fieldNames().next();
        switch (s) {
            case "and": {
                return getLogicalOperation(operation.get(operationType), "and");
            }
            case "or": {
                return getLogicalOperation(operation.get(operationType), "or");
            }
            case "not": {
                return getLogicalOperationNot(operation.get(operationType), "not");
            }
            default: {
                return getSimpleCondition(operation);
            }
        }
    }
    
    private static List<String> getLogicalOperation(final JsonNode operation, final String operationCode) {
        final int operations = operation.size();
        final List<String> results = new ArrayList<String>();
        for (int index = 0; index < operations; ++index) {
            results.addAll(getLogicalOperation(operation.get(index)));
            if (index != operations - 1) {
                results.add(" " + operationCode.toUpperCase() + " ");
            }
        }
        return results;
    }
    
    private static List<String> getLogicalOperationNot(final JsonNode operation, final String operationCode) {
        final List<String> results = new ArrayList<String>();
        results.add(operationCode.toUpperCase() + " ( ");
        results.addAll(getLogicalOperation(operation));
        results.add(" ) ");
        return results;
    }
    
    private static List<String> getSimpleCondition(final JsonNode operation) throws CapabilityException {
        final List<String> resultSet = new ArrayList<String>();
        final String operationName = operation.get("oper").asText();
        final String attributeName = getWhereClauseAttributeName(operation);
        final List<String> valueList = new ArrayList<String>();
        String operationSymbol = "";
        final String s = operationName;
        switch (s) {
            case "gt": {
                operationSymbol = ">";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "lt": {
                operationSymbol = "<";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "gte": {
                operationSymbol = ">=";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "lte": {
                operationSymbol = "<=";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "eq": {
                operationSymbol = "=";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "neq": {
                operationSymbol = "!=";
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationSymbol + " " + value);
                break;
            }
            case "like": {
                final String value = operation.get("value").asText();
                resultSet.add(attributeName + " " + operationName.toUpperCase() + " " + value);
                break;
            }
            case "between": {
                final JsonNode values = operation.get("values");
                for (final JsonNode valueNode : values) {
                    valueList.add(valueNode.asText());
                }
                resultSet.add(attributeName + " " + operationName.toUpperCase() + " " + valueList.get(0) + " AND " + valueList.get(1));
                break;
            }
            case "in": {
                final JsonNode values = operation.get("values");
                for (final JsonNode valueNode : values) {
                    valueList.add(valueNode.asText());
                }
                resultSet.add(attributeName + " " + operationName.toUpperCase() + " ( " + Utility.join(valueList) + " ) ");
                break;
            }
        }
        return resultSet;
    }
    
    private static String getWhereClauseAttributeName(final JsonNode operation) {
        final String attributeName = operation.get("attr").asText();
        return attributeName;
    }
    
    static {
        logger = Logger.getLogger((Class)CQUtility.class);
    }
}
