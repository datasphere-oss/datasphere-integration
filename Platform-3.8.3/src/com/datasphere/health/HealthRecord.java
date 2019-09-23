package com.datasphere.health;

import java.util.*;
import com.fasterxml.jackson.databind.node.*;
import com.datasphere.hdstore.*;
import java.io.*;

public class HealthRecord
{
    String id;
    private long startTime;
    private long endTime;
    private int clusterSize;
    private boolean derbyAlive;
    private Object elasticSearch;
    private int agentCount;
    private Map<String, AppHealth> appHealthMap;
    private Map<String, ServerHealth> serverHealthMap;
    private Map<String, SourceHealth> sourceHealthMap;
    private Map<String, HStoreHealth> waStoreHealthMap;
    private Map<String, TargetHealth> targetHealthMap;
    private Map<String, CacheHealth> cacheHealthMap;
    private Map<String, KafkaHealth> kafkaHealthMap;
    private List<StateChange> stateChangeList;
    private List<Issue> issuesList;
    public static final String FIELD_UUID = "id";
    public static final String FIELD_START_TIME = "startTime";
    public static final String FIELD_END_TIME = "endTime";
    public static final String FIELD_MEMORY = "memory";
    public static final String FIELD_CPU = "cpu";
    public static final String FIELD_SERVERS = "servers";
    public static final String HEALTH_SCHEMA = "{  \"context\": [     { \"name\": \"id\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" }  ]}";
    
    public HealthRecord() {
        this.clusterSize = -1;
        this.appHealthMap = new HashMap<String, AppHealth>();
        this.serverHealthMap = new HashMap<String, ServerHealth>();
        this.sourceHealthMap = new HashMap<String, SourceHealth>();
        this.waStoreHealthMap = new HashMap<String, HStoreHealth>();
        this.targetHealthMap = new HashMap<String, TargetHealth>();
        this.cacheHealthMap = new HashMap<String, CacheHealth>();
        this.kafkaHealthMap = new HashMap<String, KafkaHealth>();
        this.stateChangeList = new ArrayList<StateChange>();
    }
    
    public HealthRecord(final String uuid, final int agentCount, final int clusterSize, final boolean derbyAlive, final Object elasticSearch, final long endTime, final long startTime, final Map<String, AppHealth> appHealthMap, final Map<String, CacheHealth> cacheHealthMap, final List<Issue> issuesList, final Map<String, ServerHealth> serverHealthMap, final Map<String, SourceHealth> sourceHealthMap, final List<StateChange> stateChangeList, final Map<String, TargetHealth> targetHealthMap, final Map<String, HStoreHealth> waStoreHealthMap, final Map<String, KafkaHealth> kafkaHealthMap) {
        this.clusterSize = -1;
        this.appHealthMap = new HashMap<String, AppHealth>();
        this.serverHealthMap = new HashMap<String, ServerHealth>();
        this.sourceHealthMap = new HashMap<String, SourceHealth>();
        this.waStoreHealthMap = new HashMap<String, HStoreHealth>();
        this.targetHealthMap = new HashMap<String, TargetHealth>();
        this.cacheHealthMap = new HashMap<String, CacheHealth>();
        this.kafkaHealthMap = new HashMap<String, KafkaHealth>();
        this.stateChangeList = new ArrayList<StateChange>();
        this.id = uuid;
        this.endTime = endTime;
        this.startTime = startTime;
        this.agentCount = agentCount;
        this.derbyAlive = derbyAlive;
        this.clusterSize = clusterSize;
        this.elasticSearch = elasticSearch;
        this.appHealthMap = appHealthMap;
        this.cacheHealthMap = cacheHealthMap;
        this.serverHealthMap = serverHealthMap;
        this.sourceHealthMap = sourceHealthMap;
        this.targetHealthMap = targetHealthMap;
        this.waStoreHealthMap = waStoreHealthMap;
        this.kafkaHealthMap = kafkaHealthMap;
        this.issuesList = issuesList;
        this.stateChangeList = stateChangeList;
    }
    
    @Override
    public String toString() {
        final StringBuilder s = new StringBuilder();
        s.append("{\n").append("\t'id': ").append(this.id).append(",\n").append("\t'startTime': ").append(this.startTime).append(",\n").append("\t'endTime': ").append(this.endTime).append(",\n").append("\t'clusterSize': ").append(this.clusterSize).append(",\n").append("\t'derbyAlive': ").append(this.derbyAlive).append(",\n").append("\t'elasticSearchAlive': ").append(this.elasticSearch).append(",\n").append("\t'agentCount': ").append(this.agentCount).append(",\n");
        s.append("\t'appHealthMap': { \n");
        for (final String name : this.appHealthMap.keySet()) {
            final AppHealth h = this.appHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqAppName': ").append(h.fqAppName).append(",\n").append("\t\t\t'status': '").append(h.status).append("',\n").append("\t\t\t'lastModifiedDate': ").append(h.lastModifiedTime).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'serverHealthMap': { \n");
        for (final String name : this.serverHealthMap.keySet()) {
            final ServerHealth h2 = this.serverHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqServerName': ").append(h2.fqServerName).append(",\n").append("\t\t\t'cpu': '").append(h2.cpu).append("',\n").append("\t\t\t'memory': ").append(h2.memory).append("\n").append("\t\t\t'diskFree': ").append(h2.diskFree).append("\n").append("\t\t\t'elasticsearchFree': ").append(h2.elasticsearchFree).append("\n").append("\t\t}\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'sourceHealthMap': { \n");
        for (final String name : this.sourceHealthMap.keySet()) {
            final SourceHealth h3 = this.sourceHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqSourceName': ").append(h3.fqSourceName).append(",\n").append("\t\t\t'eventRate': ").append(h3.eventRate).append(",\n").append("\t\t\t'lastEventTime': ").append(h3.lastEventTime).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'waStoreHealthMap': { \n");
        for (final String name : this.waStoreHealthMap.keySet()) {
            final HStoreHealth h4 = this.waStoreHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqWAStoreName': ").append(h4.fqWAStoreName).append(",\n").append("\t\t\t'writeRate': ").append(h4.writeRate).append(",\n").append("\t\t\t'lastWriteTime': ").append(h4.lastWriteTime).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'targetHealthMap': { \n");
        for (final String name : this.targetHealthMap.keySet()) {
            final TargetHealth h5 = this.targetHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqTargetName': ").append(h5.fqTargetName).append(",\n").append("\t\t\t'eventRate': ").append(h5.eventRate).append(",\n").append("\t\t\t'lastWriteTime': ").append(h5.lastWriteTime).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'cacheHealthMap': { \n");
        for (final String name : this.cacheHealthMap.keySet()) {
            final CacheHealth h6 = this.cacheHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqCacheName': ").append(h6.fqCacheName).append(",\n").append("\t\t\t'size': '").append(h6.size).append("',\n").append("\t\t\t'lastRefresh': ").append(h6.lastRefresh).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'kafkaHealthMap': { \n");
        for (final String name : this.kafkaHealthMap.keySet()) {
            final KafkaHealth h7 = this.kafkaHealthMap.get(name);
            s.append("\t\t'").append(name).append("': {\n").append("\t\t\t'fqKafkaName': ").append(h7.fqKafkaName).append(",\n").append("\t\t\t'latestActivity': ").append(h7.latestActivity).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t},\n");
        s.append("\t'stateChangeList': [ \n");
        for (final StateChange h8 : this.stateChangeList) {
            s.append("\t\t{\n").append("\t\t\t'fqName': ").append(h8.fqName).append(",\n").append("\t\t\t'previousStatus': '").append(h8.previousStatus).append("',\n").append("\t\t\t'currentStatus': '").append(h8.currentStatus).append("',\n").append("\t\t\t'timestamp': ").append(h8.timestamp).append("\n").append("\t\t},\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t],\n");
        s.append("\t'issuesList': [ \n");
        for (final Issue i : this.issuesList) {
            s.append("\t\t{\n").append("\t\t\t'fqName': ").append(i.fqName).append(",\n").append("\t\t\t'componentType': '").append(i.componentType).append("',\n").append("\t\t\t'issue': ").append(i.issue).append(",\n").append("\t\t}\n");
        }
        s.deleteCharAt(s.length() - 2).append("\t]\n");
        s.append("}");
        final String result = s.toString().replace('\'', '\"');
        return result;
    }
    
    public int getAgentCount() {
        return this.agentCount;
    }
    
    public void setAgentCount(final int agentCount) {
        this.agentCount = agentCount;
    }
    
    public Map<String, AppHealth> getAppHealthMap() {
        return this.appHealthMap;
    }
    
    public void setAppHealthMap(final Map<String, AppHealth> appHealthMap) {
        this.appHealthMap = appHealthMap;
    }
    
    public Map<String, CacheHealth> getCacheHealthMap() {
        return this.cacheHealthMap;
    }
    
    public void setCacheHealthMap(final Map<String, CacheHealth> cacheHealthMap) {
        this.cacheHealthMap = cacheHealthMap;
    }
    
    public int getClusterSize() {
        return this.clusterSize;
    }
    
    public void setClusterSize(final int clusterSize) {
        this.clusterSize = clusterSize;
    }
    
    public boolean isDerbyAlive() {
        return this.derbyAlive;
    }
    
    public void setDerbyAlive(final boolean derbyAlive) {
        this.derbyAlive = derbyAlive;
    }
    
    public Object getElasticSearch() {
        return this.elasticSearch;
    }
    
    public void setElasticSearch(final Object elasticSearch) {
        this.elasticSearch = elasticSearch;
    }
    
    public long getEndTime() {
        return this.endTime;
    }
    
    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }
    
    public List<Issue> getIssuesList() {
        return this.issuesList;
    }
    
    public void setIssuesList(final List<Issue> issuesList) {
        this.issuesList = issuesList;
    }
    
    public Map<String, ServerHealth> getServerHealthMap() {
        return this.serverHealthMap;
    }
    
    public void setServerHealthMap(final Map<String, ServerHealth> serverHealthMap) {
        this.serverHealthMap = serverHealthMap;
    }
    
    public Map<String, SourceHealth> getSourceHealthMap() {
        return this.sourceHealthMap;
    }
    
    public void setSourceHealthMap(final Map<String, SourceHealth> sourceHealthMap) {
        this.sourceHealthMap = sourceHealthMap;
    }
    
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }
    
    public List<StateChange> getStateChangeList() {
        return this.stateChangeList;
    }
    
    public void setStateChangeList(final List<StateChange> stateChangeList) {
        this.stateChangeList = stateChangeList;
    }
    
    public Map<String, TargetHealth> getTargetHealthMap() {
        return this.targetHealthMap;
    }
    
    public void setTargetHealthMap(final Map<String, TargetHealth> targetHealthMap) {
        this.targetHealthMap = targetHealthMap;
    }
    
    public String getId() {
        return this.id;
    }
    
    public void setId(final String uuid) {
        this.id = uuid;
    }
    
    public Map<String, HStoreHealth> getWaStoreHealthMap() {
        return this.waStoreHealthMap;
    }
    
    public void setWaStoreHealthMap(final Map<String, HStoreHealth> waStoreHealthMap) {
        this.waStoreHealthMap = waStoreHealthMap;
    }
    
    public HD gethd() {
        final ObjectNode objnode = (ObjectNode)Utility.objectMapper.valueToTree((Object)this);
        final HD hd = new HD();
        hd.setAll(objnode);
        return hd;
    }
    
    public ServerHealth getServerHealth(final String fqServerName) {
        return this.serverHealthMap.get(fqServerName);
    }
    
    public void putServerHealth(final ServerHealth serverHealth) {
        this.serverHealthMap.put(serverHealth.fqServerName, serverHealth);
    }
    
    public SourceHealth getSourceHealth(final String fqSourceName) {
        return this.sourceHealthMap.get(fqSourceName);
    }
    
    public void putSourceHealth(final SourceHealth sourceHealth) {
        this.sourceHealthMap.put(sourceHealth.fqSourceName, sourceHealth);
    }
    
    public HStoreHealth getHDStoreHealth(final String fqHDStoreName) {
        return this.waStoreHealthMap.get(fqHDStoreName);
    }
    
    public void putHDStoreHealth(final HStoreHealth hdStoreHealth) {
        this.waStoreHealthMap.put(hdStoreHealth.fqWAStoreName, hdStoreHealth);
    }
    
    public TargetHealth getTargetHealth(final String fqTargetName) {
        return this.targetHealthMap.get(fqTargetName);
    }
    
    public void putTargetHealth(final TargetHealth targetHealth) {
        this.targetHealthMap.put(targetHealth.fqTargetName, targetHealth);
    }
    
    public CacheHealth getCacheHealth(final String fqCacheName) {
        return this.cacheHealthMap.get(fqCacheName);
    }
    
    public void putCacheHealth(final CacheHealth cacheHealth) {
        this.cacheHealthMap.put(cacheHealth.fqCacheName, cacheHealth);
    }
}
