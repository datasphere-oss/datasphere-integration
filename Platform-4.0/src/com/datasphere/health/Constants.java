package com.datasphere.health;

public class Constants
{
    public static final String greaterThan = "gt";
    public static final String lessThan = "lt";
    public static final String equalTo = "eq";
    public static final String timeStamp = "startTime";
    public static final String id = "id";
    public static final String selectInTimeRange = "{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\":  { \"and\": [{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s },{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s }]}}";
    public static final String selectByUUID = "{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\": { \"oper\": \"%s\", \"attr\": \"%s\", \"value\": \"%s\" }}";
    public static final String selectByCount = "{ \"select\":[ \"*\" ],\"from\":[ \"%s\" ],\"orderby\":[{\"attr\" : \"%s\", \"ascending\" : %b}]  }";
    public static final String selectIssuesListForDateRange = "{ \"select\": [ \"issuesList\",\"startTime\" ],\"from\":   [ \"%s\" ],\"orderby\":[{\"attr\" : \"startTime\", \"ascending\" : false}] , \"where\":  { \"and\": [{ \"oper\": \"gt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"lt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"neq\", \"attr\": \"issuesList\", \"value\": null }]}}";
    public static final String selectStatusChangesListForDateRange = "{ \"select\": [ \"stateChangeList\",\"startTime\" ],\"from\":   [ \"%s\" ],\"orderby\":[{\"attr\" : \"startTime\", \"ascending\" : false}] , \"where\":  { \"and\": [{ \"oper\": \"gt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"lt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"neq\", \"attr\": \"stateChangeList\", \"value\": null }]}}";
}
