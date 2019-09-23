package com.datasphere.hdstore.elasticsearch;

import com.datasphere.hdstore.base.*;
import org.apache.log4j.*;
import java.util.regex.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.*;
import java.io.*;
import com.google.common.base.*;
import org.elasticsearch.action.search.*;
import com.datasphere.hdstore.exceptions.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import com.datasphere.hdstore.constants.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.sort.*;
import com.datasphere.hd.*;
import java.util.*;
import com.fasterxml.jackson.databind.node.*;

class HDQuery extends HDQueryBase<HDStore>
{
    private static final Class<HDQuery> thisClass;
    private static final Logger logger;
    private static final String META_CHARACTERS = ".^$?+*\\[\\]\\{\\}\\(\\)";
    private static final Pattern REGEX_META_CHARACTERS;
    private static final String REPLACE_META_CHARACTERS = "[$0]";
    private static final Pattern REGEX_PERCENT;
    private static final String REPLACE_PERCENT = "$1.*";
    private static final Pattern REGEX_UNDERSCORE;
    private static final String REPLACE_UNDERSCORE = "$1.";
    private static final Pattern REGEX_ESCAPED_LIKE;
    private static final String REPLACE_ESCAPED_LIKE = "$1";
    private static final Pattern REGEX_META_BACKSLASH;
    private static final String REPLACE_META_BACKSLASH = "[\\\\$0]";
    private static final Pattern REGEX_LITERAL_META_CHARS;
    private static final String REPLACE_LITERAL_META_CHARS = "[\\\\$1]";
    private static final Map<String, String> mappedAttributeNames;
    private final SearchRequestBuilder searchRequest;
    private boolean containsAggregations;
    private TermsAggregationBuilder groupByAggregations;
    
    HDQuery(final JsonNode queryJson, final HDStore... hdStores) throws CapabilityException {
        super(queryJson, Arrays.asList(hdStores));
        this.containsAggregations = false;
        this.groupByAggregations = null;
        final HDStore hdStore = this.getHDStore(0);
        final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
        this.searchRequest = manager.prepareSearch(hdStore, this.getQuery());
        if (this.searchRequest != null) {
            if (HDQuery.logger.isDebugEnabled()) {
                HDQuery.logger.debug((Object)String.format("HDStore  query:   '%s'", queryJson));
            }
            this.processProjection();
            this.processFiltration();
            this.processSortOrder();
            if (HDQuery.logger.isDebugEnabled()) {
                String queryString = this.searchRequest.toString();
                try {
                    queryString = Utility.objectMapper.readTree(queryString).toString();
                }
                catch (IOException ex) {}
                final String indexList = Joiner.on(',').join((Object[])((SearchRequest)this.searchRequest.request()).indices());
                final String typeList = Joiner.on(',').join((Object[])((SearchRequest)this.searchRequest.request()).types());
                HDQuery.logger.debug((Object)String.format("Elasticsearch query:   '%s'", queryString));
                HDQuery.logger.debug((Object)String.format("Elasticsearch indices: '%s'", indexList));
                HDQuery.logger.debug((Object)String.format("Elasticsearch types:   '%s'", typeList));
            }
        }
    }
    
    HDQuery(final JsonNode queryJson, final String feature, final HDStore... hdStores) {
        super(queryJson, Arrays.asList(hdStores));
        this.containsAggregations = false;
        this.groupByAggregations = null;
        this.searchRequest = null;
    }
    
    static QueryBuilder getLogicalOperation(final JsonNode operation) throws CapabilityException {
        final String s;
        final String operationType = s = operation.fieldNames().next();
        switch (s) {
            case "and": {
                return getLogicalOperationAnd(operation.get(operationType));
            }
            case "or": {
                return getLogicalOperationOr(operation.get(operationType));
            }
            case "not": {
                return getLogicalOperationNot(operation.get(operationType));
            }
            default: {
                return getSimpleCondition(operation);
            }
        }
    }
    
    private static QueryBuilder getLogicalOperationAnd(final JsonNode operation) {
        final QueryBuilder[] filters = getFilterArray(operation);
        final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (final QueryBuilder filter : filters) {
            boolQueryBuilder.must(filter);
        }
        return (QueryBuilder)boolQueryBuilder;
    }
    
    private static QueryBuilder getLogicalOperationOr(final JsonNode operation) {
        final QueryBuilder[] filters = getFilterArray(operation);
        final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (final QueryBuilder filter : filters) {
            boolQueryBuilder.should(filter);
        }
        return (QueryBuilder)boolQueryBuilder;
    }
    
    private static QueryBuilder[] getFilterArray(final JsonNode operation) {
        final int operations = operation.size();
        final QueryBuilder[] filters = new QueryBuilder[operation.size()];
        for (int index = 0; index < operations; ++index) {
            filters[index] = getLogicalOperation(operation.get(index));
        }
        return filters;
    }
    
    private static QueryBuilder getLogicalOperationNot(final JsonNode operation) {
        final QueryBuilder filter = getLogicalOperation(operation);
        return (QueryBuilder)QueryBuilders.boolQuery().mustNot(filter);
    }
    
    private static QueryBuilder getSimpleCondition(final JsonNode operation) throws CapabilityException {
        final String operationName = operation.get("oper").asText();
        final String attributeName = getWhereClauseAttributeName(operation);
        QueryBuilder result = null;
        final String s = operationName;
        switch (s) {
            case "gt":
            case "lt":
            case "gte":
            case "lte": {
                final JsonNode value = getOperationValue(operation, attributeName, "value");
                result = getSimpleConditionBinaryCompare(operationName, attributeName, value);
                break;
            }
            case "eq":
            case "neq": {
                final JsonNode value = getOperationValue(operation, attributeName, "value");
                result = getSimpleConditionBinaryEquality(operationName, attributeName, value);
                break;
            }
            case "like":
            case "regex": {
                final JsonNode value = getOperationValue(operation, attributeName, "value");
                result = getSimpleConditionPatternMatch(operationName, attributeName, value);
                break;
            }
            case "between": {
                final JsonNode values = getOperationValue(operation, attributeName, "values");
                result = getSimpleConditionTrinary(operationName, attributeName, values.get(0), values.get(1));
                break;
            }
            case "in": {
                final JsonNode values = getOperationValue(operation, attributeName, "values");
                result = getSimpleConditionSet(operationName, attributeName, values);
                break;
            }
        }
        return result;
    }
    
    private static JsonNode getOperationValue(final JsonNode operation, final String attributeName, final String valueName) {
        JsonNode value = operation.get(valueName);
        if ("_all".equals(attributeName)) {
            if (value.isTextual()) {
                value = (JsonNode)TextNode.valueOf(value.asText().toLowerCase());
            }
            else if (value.isArray()) {
                final ArrayNode valueArray = (ArrayNode)value;
                for (int index = 0; index < valueArray.size(); ++index) {
                    final JsonNode node = valueArray.get(index);
                    if (node.isTextual()) {
                        valueArray.set(index, (JsonNode)TextNode.valueOf(node.asText().toLowerCase()));
                    }
                }
                value = (JsonNode)valueArray;
            }
        }
        return value;
    }
    
    private static String getWhereClauseAttributeName(final JsonNode operation) {
        final String attributeName = operation.get("attr").asText();
        final String mappedAttributeName = HDQuery.mappedAttributeNames.get(attributeName);
        return (mappedAttributeName == null) ? attributeName : mappedAttributeName;
    }
    
    private static QueryBuilder getSimpleConditionBinaryCompare(final String operationName, final String attributeName, final JsonNode value) {
        RangeQueryBuilder result = QueryBuilders.rangeQuery(attributeName);
        switch (operationName) {
            case "gt": {
                result = result.gt(getNodeValue(value));
                break;
            }
            case "lt": {
                result = result.lt(getNodeValue(value));
                break;
            }
            case "gte": {
                result = result.gte(getNodeValue(value));
                break;
            }
            case "lte": {
                result = result.lte(getNodeValue(value));
                break;
            }
        }
        return (QueryBuilder)result;
    }
    
    private static QueryBuilder getSimpleConditionBinaryEquality(final String operationName, final String attributeName, final JsonNode value) {
        final Object valueObject = getNodeValue(value);
        QueryBuilder result = (QueryBuilder)((valueObject == null) ? QueryBuilders.boolQuery().mustNot((QueryBuilder)QueryBuilders.existsQuery(attributeName)) : QueryBuilders.termsQuery(attributeName, new Object[] { valueObject }));
        if ("neq".equals(operationName)) {
            result = (QueryBuilder)QueryBuilders.boolQuery().mustNot(result);
        }
        return result;
    }
    
    private static QueryBuilder getSimpleConditionPatternMatch(final String operationName, final String attributeName, final JsonNode value) {
        String valueString = getNodeValue(value).toString();
        if ("like".equals(operationName)) {
            valueString = HDQuery.REGEX_META_CHARACTERS.matcher(valueString).replaceAll("[$0]");
            valueString = HDQuery.REGEX_PERCENT.matcher(valueString).replaceAll("$1.*");
            for (int loop = 1; loop <= 2; ++loop) {
                valueString = HDQuery.REGEX_UNDERSCORE.matcher(valueString).replaceAll("$1.");
            }
            valueString = HDQuery.REGEX_ESCAPED_LIKE.matcher(valueString).replaceAll("$1");
            valueString = HDQuery.REGEX_META_BACKSLASH.matcher(valueString).replaceAll("[\\\\$0]");
            valueString = HDQuery.REGEX_LITERAL_META_CHARS.matcher(valueString).replaceAll("[\\\\$1]");
            HDQuery.logger.debug((Object)String.format("Transformed match filter from LIKE '%s' to regular expression '%s'", getNodeValue(value).toString(), valueString));
        }
        return (QueryBuilder)QueryBuilders.regexpQuery(attributeName, valueString);
    }
    
    private static QueryBuilder getSimpleConditionTrinary(final String operationName, final String attributeName, final JsonNode jsonNodeLeft, final JsonNode jsonNodeRight) {
        QueryBuilder result = null;
        if ("between".equals(operationName)) {
            result = (QueryBuilder)QueryBuilders.rangeQuery(attributeName).gte(getNodeValue(jsonNodeLeft)).lte(getNodeValue(jsonNodeRight));
        }
        return result;
    }
    
    private static QueryBuilder getSimpleConditionSet(final String operationName, final String attributeName, final JsonNode values) {
        QueryBuilder result = null;
        if ("in".equals(operationName)) {
            final Collection<Object> valueList = new ArrayList<Object>(values.size());
            for (final JsonNode valueNode : values) {
                final Object valueObject = getNodeValue(valueNode);
                valueList.add(valueObject);
            }
            result = (QueryBuilder)QueryBuilders.termsQuery(attributeName, (Collection)valueList);
        }
        return result;
    }
    
    private static Object getNodeValue(final JsonNode value) {
        Object result = null;
        switch (value.getNodeType()) {
            case STRING: {
                result = value.asText();
                break;
            }
            case BOOLEAN: {
                result = value.asBoolean();
                break;
            }
            case NUMBER: {
                result = (value.isIntegralNumber() ? value.asLong() : value.asDouble());
                break;
            }
        }
        return result;
    }
    
    private void processProjection() throws CapabilityException {
        final JsonNode attributes = this.getQuery().get("select");
        if (!"*".equals(attributes.get(0).asText())) {
            this.buildGroupByAggregations();
            final List<String> fetchSource = new ArrayList<String>(attributes.size());
            boolean hasSource = false;
            for (int index = 0; index < attributes.size(); ++index) {
                final JsonNode attribute = attributes.get(index);
                if (attribute.isValueNode()) {
                    fetchSource.add(attribute.asText());
                    hasSource = true;
                }
                else {
                    this.processProjectionFunction(attribute);
                    this.containsAggregations = true;
                }
            }
            if (hasSource) {
                this.searchRequest.setFetchSource((String[])fetchSource.toArray(new String[fetchSource.size()]), (String[])null);
            }
        }
    }
    
    private void buildGroupByAggregations() {
        final JsonNode query = this.getQuery();
        final JsonNode groupByNodes = query.get("groupby");
        final JsonNode orderByNodes = query.get("orderby");
        if (groupByNodes != null) {
            for (final JsonNode groupByNode : groupByNodes) {
                final String groupByAttributeName = groupByNode.asText();
                final String groupByAggregateName = groupByAttributeName + ':' + "groupby";
                final Terms.Order order = getSortOrder(orderByNodes, groupByAttributeName);
                final TermsAggregationBuilder groupByAggregation = ((TermsAggregationBuilder)AggregationBuilders.terms(groupByAggregateName).field(groupByAttributeName)).size(Integer.MAX_VALUE).order(order);
                if (this.groupByAggregations == null) {
                    this.searchRequest.addAggregation((AggregationBuilder)groupByAggregation);
                }
                else {
                    this.groupByAggregations.subAggregation((AggregationBuilder)groupByAggregation);
                }
                this.groupByAggregations = groupByAggregation;
            }
        }
    }
    
    private static Terms.Order getSortOrder(final JsonNode orderByNodes, final String groupByAttributeName) {
        if (orderByNodes != null) {
            for (final JsonNode orderByNode : orderByNodes) {
                final String orderByAttributeName = orderByNode.get("attr").asText();
                if (groupByAttributeName.equals(orderByAttributeName)) {
                    final boolean orderByAscending = orderByNode.get("ascending").asBoolean(true);
                    return Terms.Order.term(orderByAscending);
                }
            }
        }
        return Terms.Order.term(true);
    }
    
    private void processProjectionFunction(final JsonNode function) throws CapabilityException {
        final String functionName = function.get("oper").asText();
        final String attributeName = function.get("attr").asText();
        final String s = functionName;
        switch (s) {
            case "min":
            case "max":
            case "count":
            case "sum":
            case "avg": {
                this.addAggregation(functionName, attributeName);
                break;
            }
            case "first": {
                throw new CapabilityException(Capability.AGGREGATION_FIRST.name());
            }
            case "last": {
                throw new CapabilityException(Capability.AGGREGATION_LAST.name());
            }
        }
    }
    
    private void addAggregation(final String functionName, final String attributeName) {
        AbstractAggregationBuilder aggregation = null;
        final String aggregationName = attributeName + ':' + functionName;
        switch (functionName) {
            case "min": {
                aggregation = (AbstractAggregationBuilder)AggregationBuilders.min(aggregationName).field(attributeName);
                break;
            }
            case "max": {
                aggregation = (AbstractAggregationBuilder)AggregationBuilders.max(aggregationName).field(attributeName);
                break;
            }
            case "count": {
                aggregation = (AbstractAggregationBuilder)AggregationBuilders.count(aggregationName).field(attributeName);
                break;
            }
            case "sum": {
                aggregation = (AbstractAggregationBuilder)AggregationBuilders.sum(aggregationName).field(attributeName);
                break;
            }
            case "avg": {
                aggregation = (AbstractAggregationBuilder)AggregationBuilders.avg(aggregationName).field(attributeName);
                break;
            }
        }
        if (this.groupByAggregations != null) {
            this.groupByAggregations.subAggregation((AggregationBuilder)aggregation);
        }
        else {
            this.searchRequest.addAggregation((AggregationBuilder)aggregation);
        }
    }
    
    private void processFiltration() {
        final JsonNode whereClause = this.getQuery().get("where");
        QueryBuilder queryBuilder = (QueryBuilder)QueryBuilders.matchAllQuery();
        if (whereClause != null) {
            final QueryBuilder filter = getLogicalOperation(whereClause);
            queryBuilder = (QueryBuilder)QueryBuilders.boolQuery().must(queryBuilder).filter(filter);
        }
        this.searchRequest.setQuery(queryBuilder);
    }
    
    private void processSortOrder() {
        final JsonNode attributes = this.getQuery().get("orderby");
        if (attributes != null && this.getQuery().get("groupby") == null) {
            for (final JsonNode attribute : attributes) {
                final String field = attribute.get("attr").asText();
                final JsonNode ascending = attribute.get("ascending");
                final SortOrder order = (ascending == null || ascending.asBoolean(true)) ? SortOrder.ASC : SortOrder.DESC;
                this.searchRequest.addSort(field, order);
            }
        }
    }
    
    @Override
    public HDQueryResult execute() throws CapabilityException {
        return new ElasticSearchQueryResult(this.searchRequest, ((HDStoreManager)this.getHDStore(0).getManager()).getClient(), this.containsAggregations);
    }
    
    static {
        thisClass = HDQuery.class;
        logger = Logger.getLogger((Class)HDQuery.thisClass);
        REGEX_META_CHARACTERS = Pattern.compile("[.^$?+*\\[\\]\\{\\}\\(\\)]");
        REGEX_PERCENT = Pattern.compile("(^|[^\\\\])[%]+");
        REGEX_UNDERSCORE = Pattern.compile("(^|[^\\\\])[_]");
        REGEX_ESCAPED_LIKE = Pattern.compile("[\\\\]([%_])");
        REGEX_META_BACKSLASH = Pattern.compile("[\\\\]");
        REGEX_LITERAL_META_CHARS = Pattern.compile("[\\[]([.^$?+*\\[\\]\\{\\}\\(\\)])[\\]]");
        (mappedAttributeNames = new HashMap<String, String>(1)).put("*any", "_all");
    }
}
