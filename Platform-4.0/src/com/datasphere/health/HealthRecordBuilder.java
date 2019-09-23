package com.datasphere.health;

import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.monitor.*;
import java.util.concurrent.*;
import java.util.*;
import com.datasphere.runtime.meta.*;

public class HealthRecordBuilder
{
    private UUID uuid;
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
    Map<String, Map<String, CountAndTimeStamp>> cacheSize;
    Map<String, Map<String, CountAndTimeStamp>> targetEventCount;
    Map<String, Map<String, CountAndTimeStamp>> waStoreOutputCount;
    Map<String, Map<String, CountAndTimeStamp>> sourceEventCount;
    
    public HealthRecordBuilder() {
        this.uuid = new UUID(System.currentTimeMillis());
        this.appHealthMap = new HashMap<String, AppHealth>();
        this.serverHealthMap = new HashMap<String, ServerHealth>();
        this.sourceHealthMap = new HashMap<String, SourceHealth>();
        this.waStoreHealthMap = new HashMap<String, HStoreHealth>();
        this.targetHealthMap = new HashMap<String, TargetHealth>();
        this.cacheHealthMap = new HashMap<String, CacheHealth>();
        this.kafkaHealthMap = new HashMap<String, KafkaHealth>();
        this.stateChangeList = new ArrayList<StateChange>();
        this.issuesList = new ArrayList<Issue>();
        this.cacheSize = new HashMap<String, Map<String, CountAndTimeStamp>>();
        this.targetEventCount = new HashMap<String, Map<String, CountAndTimeStamp>>();
        this.waStoreOutputCount = new HashMap<String, Map<String, CountAndTimeStamp>>();
        this.sourceEventCount = new HashMap<String, Map<String, CountAndTimeStamp>>();
    }
    
    public HealthRecordBuilder setUuid(final UUID uuid) {
        this.uuid = uuid;
        return this;
    }
    
    public HealthRecordBuilder setStartTime(final long startTime) {
        this.startTime = startTime;
        return this;
    }
    
    public HealthRecordBuilder setClusterSize(final int clusterSize) {
        this.clusterSize = clusterSize;
        return this;
    }
    
    public HealthRecordBuilder setDerbyAlive(final boolean derbyAlive) {
        this.derbyAlive = derbyAlive;
        return this;
    }
    
    public HealthRecordBuilder setElasticSearch(final Object elasticSearch) {
        this.elasticSearch = elasticSearch;
        return this;
    }
    
    public HealthRecordBuilder setEndTime(final long endTime) {
        this.endTime = endTime;
        return this;
    }
    
    public HealthRecordBuilder setIssuesList(final List<Issue> issuesList) {
        this.issuesList = issuesList;
        return this;
    }
    
    public HealthRecordBuilder setAgentCount(final int agentCount) {
        this.agentCount = agentCount;
        return this;
    }
    
    public HealthRecordBuilder setAppHealthMap(final Map<String, AppHealth> appHealthMap) {
        this.appHealthMap = appHealthMap;
        return this;
    }
    
    public HealthRecordBuilder setCacheHealthMap(final Map<String, CacheHealth> cacheHealthMap) {
        this.cacheHealthMap = cacheHealthMap;
        return this;
    }
    
    public HealthRecordBuilder setServerHealthMap(final Map<String, ServerHealth> serverHealthMap) {
        this.serverHealthMap = serverHealthMap;
        return this;
    }
    
    public HealthRecordBuilder setSourceHealthMap(final Map<String, SourceHealth> sourceHealthMap) {
        this.sourceHealthMap = sourceHealthMap;
        return this;
    }
    
    public HealthRecordBuilder setStateChangeList(final List<StateChange> stateChangeList) {
        this.stateChangeList = stateChangeList;
        return this;
    }
    
    public HealthRecordBuilder setTargetHealthMap(final Map<String, TargetHealth> targetHealthMap) {
        this.targetHealthMap = targetHealthMap;
        return this;
    }
    
    public HealthRecordBuilder setWaStoreHealthMap(final Map<String, HStoreHealth> waStoreHealthMap) {
        this.waStoreHealthMap = waStoreHealthMap;
        return this;
    }
    
    public HealthRecord createHealthRecord() {
        return new HealthRecord(this.uuid.getUUIDString(), this.agentCount, this.clusterSize, this.derbyAlive, this.elasticSearch, this.endTime, this.startTime, this.appHealthMap, this.cacheHealthMap, this.issuesList, this.serverHealthMap, this.sourceHealthMap, this.stateChangeList, this.targetHealthMap, this.waStoreHealthMap, this.kafkaHealthMap);
    }
    
    public synchronized void addValue(final String serverID, final String fqName, final EntityType entityType, final MonitorEvent.Type eventType, final long eventTimeStamp, final String valueString, final Long valueLong) {
        if (fqName != null && (fqName.contains("MonitoringSource1") || fqName.contains("MonitoringProcessApp"))) {
            return;
        }
        if (entityType == EntityType.SERVER) {
            ServerHealth serverHealth = this.serverHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.CPU_RATE) {
                if (serverHealth == null) {
                    serverHealth = new ServerHealth(fqName);
                    this.serverHealthMap.put(fqName, serverHealth);
                }
                serverHealth.cpu = MonitorModel.renderCpuPercent(valueLong);
            }
            else if (eventType == MonitorEvent.Type.MEMORY_FREE) {
                if (serverHealth == null) {
                    serverHealth = new ServerHealth(fqName);
                    this.serverHealthMap.put(fqName, serverHealth);
                }
                serverHealth.memory = valueLong;
            }
            else if (eventType == MonitorEvent.Type.DISK_FREE) {
                if (serverHealth == null) {
                    serverHealth = new ServerHealth(fqName);
                    this.serverHealthMap.put(fqName, serverHealth);
                }
                serverHealth.diskFree = valueString;
            }
            else if (eventType == MonitorEvent.Type.ELASTICSEARCH_FREE) {
                if (serverHealth == null) {
                    serverHealth = new ServerHealth(fqName);
                    this.serverHealthMap.put(fqName, serverHealth);
                }
                serverHealth.elasticsearchFree = valueString;
            }
        }
        else if (entityType == EntityType.SOURCE) {
            SourceHealth sourceHealth = this.sourceHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.INPUT_RATE) {
                if (sourceHealth == null) {
                    sourceHealth = new SourceHealth(fqName);
                    this.sourceHealthMap.put(fqName, sourceHealth);
                }
                Map<String, CountAndTimeStamp> serversToCounts = this.sourceEventCount.get(fqName);
                if (serversToCounts == null) {
                    serversToCounts = new ConcurrentHashMap<String, CountAndTimeStamp>();
                    this.sourceEventCount.put(fqName, serversToCounts);
                }
                CountAndTimeStamp CountAndTimeStamp = serversToCounts.get(serverID);
                if (CountAndTimeStamp == null) {
                    CountAndTimeStamp = new CountAndTimeStamp(valueLong, eventTimeStamp);
                    serversToCounts.put(serverID, CountAndTimeStamp);
                }
                else {
                    if (eventTimeStamp < CountAndTimeStamp.timeStamp) {
                        return;
                    }
                    CountAndTimeStamp.count = valueLong;
                    CountAndTimeStamp.timeStamp = eventTimeStamp;
                }
                long numberOfEvents = 0L;
                for (final CountAndTimeStamp x : serversToCounts.values()) {
                    numberOfEvents += x.count;
                }
                sourceHealth.eventRate = numberOfEvents;
            }
            else if (eventType == MonitorEvent.Type.LATEST_ACTIVITY) {
                if (sourceHealth == null) {
                    sourceHealth = new SourceHealth(fqName);
                    this.sourceHealthMap.put(fqName, sourceHealth);
                }
                if (valueLong > sourceHealth.lastEventTime) {
                    sourceHealth.lastEventTime = valueLong;
                }
            }
        }
        else if (entityType == EntityType.HDSTORE) {
            HStoreHealth hdStoreHealth = this.waStoreHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.LATEST_ACTIVITY) {
                if (hdStoreHealth == null) {
                    hdStoreHealth = new HStoreHealth(fqName);
                    this.waStoreHealthMap.put(fqName, hdStoreHealth);
                }
                if (valueLong > hdStoreHealth.lastWriteTime) {
                    hdStoreHealth.lastWriteTime = valueLong;
                }
            }
            else if (eventType == MonitorEvent.Type.RATE) {
                if (hdStoreHealth == null) {
                    hdStoreHealth = new HStoreHealth(fqName);
                    this.waStoreHealthMap.put(fqName, hdStoreHealth);
                }
                Map<String, CountAndTimeStamp> serversToCounts = this.waStoreOutputCount.get(fqName);
                if (serversToCounts == null) {
                    serversToCounts = new HashMap<String, CountAndTimeStamp>();
                    this.waStoreOutputCount.put(fqName, serversToCounts);
                }
                CountAndTimeStamp CountAndTimeStamp = serversToCounts.get(serverID);
                if (CountAndTimeStamp == null) {
                    CountAndTimeStamp = new CountAndTimeStamp(valueLong, eventTimeStamp);
                    serversToCounts.put(serverID, CountAndTimeStamp);
                }
                else {
                    if (eventTimeStamp < CountAndTimeStamp.timeStamp) {
                        return;
                    }
                    CountAndTimeStamp.count = valueLong;
                    CountAndTimeStamp.timeStamp = eventTimeStamp;
                }
                long numberOfEvents = 0L;
                for (final CountAndTimeStamp x : serversToCounts.values()) {
                    numberOfEvents += x.count;
                }
                hdStoreHealth.writeRate = numberOfEvents;
            }
        }
        else if (entityType == EntityType.TARGET) {
            TargetHealth targetHealth = this.targetHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.INPUT_RATE) {
                if (targetHealth == null) {
                    targetHealth = new TargetHealth(fqName);
                    this.targetHealthMap.put(fqName, targetHealth);
                }
                Map<String, CountAndTimeStamp> serversToCounts = this.targetEventCount.get(fqName);
                if (serversToCounts == null) {
                    serversToCounts = new HashMap<String, CountAndTimeStamp>();
                    this.targetEventCount.put(fqName, serversToCounts);
                }
                CountAndTimeStamp CountAndTimeStamp = serversToCounts.get(serverID);
                if (CountAndTimeStamp == null) {
                    CountAndTimeStamp = new CountAndTimeStamp(valueLong, eventTimeStamp);
                    serversToCounts.put(serverID, CountAndTimeStamp);
                }
                else {
                    if (eventTimeStamp < CountAndTimeStamp.timeStamp) {
                        return;
                    }
                    CountAndTimeStamp.count = valueLong;
                    CountAndTimeStamp.timeStamp = eventTimeStamp;
                }
                long numberOfEvents = 0L;
                for (final CountAndTimeStamp x : serversToCounts.values()) {
                    numberOfEvents += x.count;
                }
                targetHealth.eventRate = numberOfEvents;
            }
            else if (eventType == MonitorEvent.Type.LATEST_ACTIVITY) {
                if (targetHealth == null) {
                    targetHealth = new TargetHealth(fqName);
                    this.targetHealthMap.put(fqName, targetHealth);
                }
                if (valueLong > targetHealth.lastWriteTime) {
                    targetHealth.lastWriteTime = valueLong;
                }
            }
        }
        else if (entityType == EntityType.CACHE) {
            CacheHealth cacheHealth = this.cacheHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.CACHE_SIZE) {
                if (cacheHealth == null) {
                    cacheHealth = new CacheHealth(fqName);
                    this.cacheHealthMap.put(fqName, cacheHealth);
                }
                Map<String, CountAndTimeStamp> serversToCounts = this.cacheSize.get(fqName);
                if (serversToCounts == null) {
                    serversToCounts = new HashMap<String, CountAndTimeStamp>();
                    this.cacheSize.put(fqName, serversToCounts);
                }
                CountAndTimeStamp CountAndTimeStamp = serversToCounts.get(serverID);
                if (CountAndTimeStamp == null) {
                    CountAndTimeStamp = new CountAndTimeStamp(valueLong, eventTimeStamp);
                    serversToCounts.put(serverID, CountAndTimeStamp);
                }
                else {
                    if (eventTimeStamp < CountAndTimeStamp.timeStamp) {
                        return;
                    }
                    CountAndTimeStamp.count = valueLong;
                    CountAndTimeStamp.timeStamp = eventTimeStamp;
                }
                long numberOfEvents = 0L;
                for (final CountAndTimeStamp x : serversToCounts.values()) {
                    numberOfEvents += x.count;
                }
                cacheHealth.size = numberOfEvents;
            }
            else if (eventType == MonitorEvent.Type.CACHE_REFRESH) {
                if (cacheHealth == null) {
                    cacheHealth = new CacheHealth(fqName);
                    this.cacheHealthMap.put(fqName, cacheHealth);
                }
                if (valueLong > cacheHealth.lastRefresh) {
                    cacheHealth.lastRefresh = valueLong;
                }
            }
        }
        else if (entityType == EntityType.UNKNOWN && "Kafka Cluster".equals(fqName)) {
            KafkaHealth kafkaHealth = this.kafkaHealthMap.get(fqName);
            if (eventType == MonitorEvent.Type.LATEST_ACTIVITY) {
                if (kafkaHealth == null) {
                    kafkaHealth = new KafkaHealth(fqName);
                    this.kafkaHealthMap.put(fqName, kafkaHealth);
                }
                kafkaHealth.latestActivity = valueLong;
            }
        }
    }
    
    public void addIssue(final String fqName, final String componentType, final String issue) {
        if (fqName != null && (fqName.contains("MonitoringSource1") || fqName.contains("MonitoringProcessApp"))) {
            return;
        }
        this.issuesList.add(new Issue(fqName, componentType, issue));
    }
    
    public void addStateChange(final String fqName, final String type, final long timeStamp, final String valueString) {
        if (fqName != null && (fqName.contains("MonitoringSource1") || fqName.contains("MonitoringProcessApp"))) {
            return;
        }
        final String[] fiveParts = MetaInfo.StatusInfo.extractFiveStrings(valueString);
        if (fiveParts != null) {
            this.stateChangeList.add(new StateChange(fqName, type, fiveParts[3], fiveParts[4], timeStamp));
        }
        else {
            this.stateChangeList.add(new StateChange(fqName, type, "", valueString, timeStamp));
        }
    }
    
    private class CountAndTimeStamp
    {
        public long count;
        public long timeStamp;
        
        public CountAndTimeStamp(final long count, final long timeStamp) {
            this.count = count;
            this.timeStamp = timeStamp;
        }
    }
}
