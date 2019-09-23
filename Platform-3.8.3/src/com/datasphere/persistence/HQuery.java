package com.datasphere.persistence;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import com.datasphere.distribution.Queryable;
import com.datasphere.distribution.HQuery;
import com.datasphere.distribution.HSimpleQuery;
import com.datasphere.event.SimpleEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HD;
import com.datasphere.hd.HDKey;

public class HQuery implements HQuery<HDKey, HD>, HSimpleQuery<HDKey, HD>, Serializable
{
    private static final long serialVersionUID = -2540403017766014356L;
    private static Logger logger;
    String wsName;
    String[] fields;
    Map<String, Object> filterMap;
    Map<String, FilterCondition> contextFilter;
    Long startTime;
    Long endTime;
    Object key;
    HDKey singleKey;
    boolean getLatestPerKey;
    String sortBy;
    boolean sortDescending;
    int limit;
    Integer maxResults;
    boolean useMaxResults;
    String groupBy;
    boolean getEvents;
    transient Map<HDKey, Map<String, Object>> finalResults;
    transient List<HD> simpleQueryResults;
    ResultStats globalStats;
    transient Map<String, Map<Long, Map<String, Object>>> sampledResults;
    transient TreeSet<Long> timePeriods;
    boolean doSampling;
    transient Map<Object, Map<String, Object>> unique;
    transient Map<Object, HDKey> mapper;
    transient ResultMapComparator rmc;
    private Map<String, String> primToClassMapping;
    private static Set<String> dontMergeSet;
    public int processed;
    public int accepted;
    boolean finalized;
    
    public HQuery() {
        this.startTime = null;
        this.endTime = null;
        this.key = null;
        this.singleKey = null;
        this.getLatestPerKey = true;
        this.sortBy = null;
        this.sortDescending = true;
        this.limit = -1;
        this.maxResults = null;
        this.useMaxResults = false;
        this.groupBy = null;
        this.getEvents = false;
        this.finalResults = new TreeMap<HDKey, Map<String, Object>>();
        this.simpleQueryResults = new ArrayList<HD>();
        this.globalStats = null;
        this.sampledResults = null;
        this.doSampling = false;
        this.unique = new HashMap<Object, Map<String, Object>>();
        this.mapper = new HashMap<Object, HDKey>();
        this.rmc = null;
        this.primToClassMapping = null;
        this.processed = 0;
        this.accepted = 0;
        this.finalized = false;
    }
    
    private String getBoxedClassName(final String className) {
        if (this.primToClassMapping == null) {
            (this.primToClassMapping = new HashMap<String, String>()).put(Byte.TYPE.getName(), Byte.class.getName());
            this.primToClassMapping.put(Short.TYPE.getName(), Short.class.getName());
            this.primToClassMapping.put(Integer.TYPE.getName(), Integer.class.getName());
            this.primToClassMapping.put(Long.TYPE.getName(), Long.class.getName());
            this.primToClassMapping.put(Float.TYPE.getName(), Float.class.getName());
            this.primToClassMapping.put(Double.TYPE.getName(), Double.class.getName());
            this.primToClassMapping.put(Boolean.TYPE.getName(), Boolean.class.getName());
        }
        if (this.primToClassMapping.containsKey(className)) {
            return this.primToClassMapping.get(className);
        }
        return className;
    }
    
    public HQuery(final String wsName, final long _startTime, final long _endTime) {
        this.startTime = null;
        this.endTime = null;
        this.key = null;
        this.singleKey = null;
        this.getLatestPerKey = true;
        this.sortBy = null;
        this.sortDescending = true;
        this.limit = -1;
        this.maxResults = null;
        this.useMaxResults = false;
        this.groupBy = null;
        this.getEvents = false;
        this.finalResults = new TreeMap<HDKey, Map<String, Object>>();
        this.simpleQueryResults = new ArrayList<HD>();
        this.globalStats = null;
        this.sampledResults = null;
        this.doSampling = false;
        this.unique = new HashMap<Object, Map<String, Object>>();
        this.mapper = new HashMap<Object, HDKey>();
        this.rmc = null;
        this.primToClassMapping = null;
        this.processed = 0;
        this.accepted = 0;
        this.finalized = false;
        this.wsName = wsName;
        this.startTime = _startTime;
        if (this.startTime < 100000000000L) {
            this.startTime *= 1000L;
        }
        this.endTime = _endTime;
        if (this.endTime < 100000000000L) {
            this.endTime *= 1000L;
        }
        this.getLatestPerKey = false;
    }
    
    public HQuery(final String wsName, final MetaInfo.Type contextType, final String[] fields, final Map<String, Object> filter) {
        this.startTime = null;
        this.endTime = null;
        this.key = null;
        this.singleKey = null;
        this.getLatestPerKey = true;
        this.sortBy = null;
        this.sortDescending = true;
        this.limit = -1;
        this.maxResults = null;
        this.useMaxResults = false;
        this.groupBy = null;
        this.getEvents = false;
        this.finalResults = new TreeMap<HDKey, Map<String, Object>>();
        this.simpleQueryResults = new ArrayList<HD>();
        this.globalStats = null;
        this.sampledResults = null;
        this.doSampling = false;
        this.unique = new HashMap<Object, Map<String, Object>>();
        this.mapper = new HashMap<Object, HDKey>();
        this.rmc = null;
        this.primToClassMapping = null;
        this.processed = 0;
        this.accepted = 0;
        this.finalized = false;
        this.wsName = wsName;
        if (fields != null) {
            this.fields = fields.clone();
        }
        else {
            this.fields = new String[] { "default" };
        }
        this.filterMap = filter;
        if (filter != null) {
            final Map<String, Object> insensitiveMap = (Map<String, Object>)makeCaseInsenstiveMap(filter);
            final Object startTimeVal = insensitiveMap.get("startTime");
            if (startTimeVal != null) {
                this.startTime = (long)(Object)new Double(startTimeVal.toString());
                if (this.startTime < 100000000000L) {
                    this.startTime *= 1000L;
                }
            }
            final Object endTimeVal = insensitiveMap.get("endTime");
            if (endTimeVal != null) {
                this.endTime = (long)(Object)new Double(endTimeVal.toString());
                if (this.endTime < 100000000000L) {
                    this.endTime *= 1000L;
                }
            }
            final Map<String, Object> contextFilterVals = (Map<String, Object>)insensitiveMap.get("context");
            if (contextFilterVals != null) {
                this.contextFilter = new HashMap<String, FilterCondition>();
                for (final Map.Entry<String, Object> entry : contextFilterVals.entrySet()) {
                    String className = "java.lang.String";
                    if (!"$ANY$".equals(entry.getKey())) {
                        for (final Map.Entry<String, String> fieldEntry : contextType.fields.entrySet()) {
                            if (entry.getKey().toLowerCase().equals(fieldEntry.getKey().toLowerCase())) {
                                className = fieldEntry.getValue();
                            }
                        }
                        if (className == null) {
                            throw new RuntimeException("Could not find field " + entry.getKey() + " in type " + contextType.getFullName());
                        }
                    }
                    Class<?> clazz = String.class;
                    try {
                        clazz = ClassLoader.getSystemClassLoader().loadClass(this.getBoxedClassName(className));
                    }
                    catch (ClassNotFoundException e) {
                        throw new RuntimeException("Could not load class " + className + " for field " + entry.getKey() + " in type " + contextType.getFullName());
                    }
                    final FilterCondition fc = new FilterCondition(entry.getValue().toString(), clazz);
                    this.contextFilter.put(entry.getKey(), fc);
                }
            }
            this.key = insensitiveMap.get("key");
            this.sortBy = (String)insensitiveMap.get("sortBy");
            final String sortDir = (String)insensitiveMap.get("sortDir");
            if (sortDir != null && sortDir.equalsIgnoreCase("asc")) {
                this.sortDescending = false;
            }
            final Object limitVal = insensitiveMap.get("limit");
            if (limitVal != null) {
                this.limit = Integer.parseInt(limitVal.toString());
            }
            final Object singleHDsVal = insensitiveMap.get("singleHDs");
            if (singleHDsVal != null && "true".equalsIgnoreCase(singleHDsVal.toString())) {
                this.getLatestPerKey = false;
            }
            if (!this.getLatestPerKey) {
                final Object maxResultsVal = insensitiveMap.get("maxResults");
                if (maxResultsVal != null) {
                    this.maxResults = Integer.parseInt(maxResultsVal.toString());
                }
                this.useMaxResults = (this.maxResults != null);
                this.groupBy = (String)insensitiveMap.get("groupBy");
            }
            for (final String f : this.fields) {
                if ("eventList".equals(f) || "default-allEvents".equals(f)) {
                    this.getEvents = true;
                }
            }
        }
    }
    
    public Map<String, Object> getFilterMap() {
        return this.filterMap;
    }
    
    public String[] getFields() {
        return this.fields;
    }
    
    public boolean willGetLatestPerKey() {
        return this.getLatestPerKey;
    }
    
    public boolean willGetEvents() {
        return this.getEvents;
    }
    
    public boolean requiresResultStats() {
        return this.useMaxResults;
    }
    
    private static Map makeCaseInsenstiveMap(final Map<String, Object> sensitive) {
        final Map<String, Object> insensitiveMap = NamePolicy.makeNameMap();
        for (final Map.Entry<String, Object> entry : sensitive.entrySet()) {
            insensitiveMap.put(NamePolicy.makeKey(entry.getKey()), entry.getValue());
        }
        return insensitiveMap;
    }
    
    private void adjustLatestPerKey(final HDKey wkey, final Map<String, Object> data) {
        final String key = wkey.key.toString();
        Map<String, Object> keyMap = this.unique.get(key);
        if (keyMap == null) {
            keyMap = new HashMap<String, Object>();
            this.unique.put(key, keyMap);
            this.mapper.put(key, wkey);
        }
        Integer keyNumEvents = (Integer)keyMap.get("totalEvents");
        if (keyNumEvents == null) {
            keyNumEvents = 0;
        }
        final Integer currNumEvents = (Integer)data.get("totalEvents");
        if (currNumEvents != null) {
            keyNumEvents += currNumEvents;
        }
        keyMap.put("totalEvents", keyNumEvents);
        final Long newestTS = (Long)keyMap.get("timestamp");
        if (newestTS == null || (Long)data.get("timestamp") > newestTS) {
            for (final Map.Entry<String, Object> ee : data.entrySet()) {
                if (!ee.getKey().equals("eventList") && !ee.getKey().equals("totalEvents")) {
                    keyMap.put(ee.getKey(), ee.getValue());
                }
            }
            this.mapper.put(key, wkey);
        }
        List<SimpleEvent> keySortedEvents = (List<SimpleEvent>)keyMap.get("eventList");
        if (keySortedEvents == null) {
            keySortedEvents = new ArrayList<SimpleEvent>();
        }
        final List<SimpleEvent> newEvents = (List<SimpleEvent>)data.get("eventList");
        if (newEvents != null) {
            keySortedEvents.addAll(newEvents);
            keyMap.put("eventList", keySortedEvents);
        }
    }
    
    private void finalizeLatestPerKey() {
        final EventsComparator ec = new EventsComparator();
        for (final Map.Entry<Object, Map<String, Object>> ee : this.unique.entrySet()) {
            final List<SimpleEvent> events = (List<SimpleEvent>)ee.getValue().get("eventList");
            if (events != null) {
                Collections.sort(events, ec);
            }
            this.finalResults.put(this.mapper.get(ee.getKey()), ee.getValue());
        }
    }
    
    private void sort() {
        if (this.sortBy != null) {
            (this.rmc = new ResultMapComparator(this.sortBy, this.sortDescending)).setBaseMap(this.finalResults);
            final Map<HDKey, Map<String, Object>> sortedRes = new TreeMap<HDKey, Map<String, Object>>(this.rmc);
            sortedRes.putAll(this.finalResults);
            this.finalResults = sortedRes;
        }
    }
    
    private void limit() {
        if (this.limit != -1) {
            Map<HDKey, Map<String, Object>> limitMap;
            if (this.rmc != null) {
                limitMap = new TreeMap<HDKey, Map<String, Object>>(this.rmc);
            }
            else {
                limitMap = new TreeMap<HDKey, Map<String, Object>>();
            }
            int numPuts = 0;
            for (final Map.Entry<HDKey, Map<String, Object>> entry : this.finalResults.entrySet()) {
                if (this.limit == numPuts) {
                    break;
                }
                limitMap.put(entry.getKey(), entry.getValue());
                ++numPuts;
            }
            this.finalResults = limitMap;
        }
    }
    
    private Collection<HD> getHDs(final Queryable<HDKey, HD> cache) {
        Map<HDKey, HD> hds = null;
        if (this.singleKey != null) {
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Get HD for Single Key: " + this.singleKey));
            }
            hds = new HashMap<HDKey, HD>();
            final HD w = cache.get(this.singleKey);
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Got HD for Single Key: " + w));
            }
            hds.put(this.singleKey, w);
        }
        else if (this.key != null) {
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Get HDs for key: " + this.key));
            }
            hds = cache.getIndexedEqual("key", this.key);
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Got " + hds.size() + " HDs for key: " + this.key));
            }
        }
        else if (this.startTime != null || this.endTime != null) {
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Get HDs for range: " + this.startTime + " to " + this.endTime));
            }
            hds = cache.getIndexedRange("ts", this.startTime, this.endTime);
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Got " + hds.size() + " HDs for range: " + this.startTime + " to " + this.endTime));
            }
        }
        if (hds == null) {
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)"--> Get All Local HDs");
            }
            hds = cache.localEntries();
            if (HQuery.logger.isDebugEnabled()) {
                HQuery.logger.debug((Object)("--> Got " + hds.size() + " Local HDs"));
            }
        }
        return hds.values();
    }
    
    public ResultStats getResultStats(final Iterable<HD> hds) {
        final ResultStats stat = new ResultStats();
        for (final HD hd : hds) {
            if (!this.matches(hd)) {
                continue;
            }
            if (stat.startTime == 0L || hd.hdTs < stat.startTime) {
                stat.startTime = hd.hdTs;
            }
            if (stat.endTime == 0L || hd.hdTs > stat.endTime) {
                stat.endTime = hd.hdTs;
            }
            final ResultStats resultStats = stat;
            ++resultStats.count;
        }
        return stat;
    }
    
    @Override
    public ResultStats getResultStats(final Queryable<HDKey, HD> cache) {
        return this.getResultStats(this.getHDs(cache));
    }
    
    @Override
    public void run(final Queryable<HDKey, HD> cache) {
        final Collection<HD> hds = this.getHDs(cache);
        if (hds == null) {
            return;
        }
        for (final HD hd : hds) {
            this.runOne(hd);
        }
    }
    
    public boolean matches(final HD hd) {
        final long hdTs = hd.hdTs;
        if (this.startTime != null && hdTs < this.startTime) {
            return false;
        }
        if (this.endTime != null && hdTs > this.endTime) {
            return false;
        }
        if (this.key != null && !this.key.equals(hd.key)) {
            return false;
        }
        if (this.contextFilter != null) {
            for (final Map.Entry<String, FilterCondition> entry : this.contextFilter.entrySet()) {
                if ("$ANY$".equals(entry.getKey())) {
                    for (final Map.Entry<String, Object> fieldEntry : hd.getContext().entrySet()) {
                        if (entry.getValue().evaluate(fieldEntry.getValue())) {
                            return true;
                        }
                    }
                    return false;
                }
                final Object value = hd.getContext().get(entry.getKey());
                if (!entry.getValue().evaluate(value)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    private Map<String, Object> getHDAsMap(final HD hd) {
        final Map<String, Object> result = new HashMap<String, Object>();
        result.put("timestamp", hd.getHDTs());
        result.put("uuid", hd.getUUIDString());
        result.put("key", hd.getKey());
        if (this.fields != null) {
            for (final String s : this.fields) {
                if (hd.getContext().containsKey(s)) {
                    result.put("context-" + s, hd.getContext().get(s));
                }
                else if ("eventList".equals(s)) {
                    result.put(s, hd.getEvents());
                }
                else if ("status".equals(s)) {
                    result.put(s, hd.getHDStatus());
                }
                else if ("default".equals(s)) {
                    for (final Map.Entry<String, Object> entry : hd.getContext().entrySet()) {
                        result.put("context-" + entry.getKey(), entry.getValue());
                    }
                }
                else if ("default-allEvents".equals(s)) {
                    for (final Map.Entry<String, Object> entry : hd.getContext().entrySet()) {
                        result.put("context-" + entry.getKey(), entry.getValue());
                    }
                    result.put("eventList", hd.getEvents());
                }
            }
            result.put("totalEvents", hd.numberOfEvents());
        }
        return result;
    }
    
    public void setGlobalStats(final ResultStats r) {
        this.globalStats = r;
        this.sampledResults = new HashMap<String, Map<Long, Map<String, Object>>>();
        this.timePeriods = new TreeSet<Long>();
        this.doSampling = false;
        if (this.globalStats.count != 0L && this.useMaxResults && this.maxResults < this.globalStats.count) {
            this.doSampling = true;
            final long sampleTimePeriod = (this.globalStats.endTime - this.globalStats.startTime) / this.maxResults;
            if (sampleTimePeriod == 0L) {
                this.timePeriods.add(this.globalStats.endTime);
            }
            else {
                for (long t = this.globalStats.endTime; t > this.globalStats.startTime; t -= sampleTimePeriod) {
                    this.timePeriods.add(t);
                }
            }
        }
    }
    
    private String getGroupByValue(final Map<String, Object> data) {
        String groupByValue = null;
        if (this.groupBy == null || "key".equalsIgnoreCase(this.groupBy)) {
            groupByValue = (String)data.get("key");
        }
        else {
            groupByValue = (String)data.get("context-" + this.groupBy);
        }
        if (groupByValue == null) {
            groupByValue = "<NOTSET>";
        }
        return groupByValue;
    }
    
    private Number sum(final Number a, Number b) {
        if (b == null) {
            b = 0;
        }
        if (a instanceof Byte) {
            final Byte c = (byte)(a.byteValue() + b.byteValue());
            return c;
        }
        if (a instanceof Short) {
            final Short c2 = (short)(a.shortValue() + b.shortValue());
            return c2;
        }
        if (a instanceof Integer) {
            final Integer c3 = a.intValue() + b.intValue();
            return c3;
        }
        if (a instanceof Long) {
            final Long c4 = a.longValue() + b.longValue();
            return c4;
        }
        if (a instanceof Float) {
            final Float c5 = a.floatValue() + b.floatValue();
            return c5;
        }
        if (a instanceof Double) {
            final Double c6 = a.doubleValue() + b.doubleValue();
            return c6;
        }
        return 0;
    }
    
    private Number ave(final Number a, final Long count) {
        if (a instanceof Byte) {
            final Byte c = (byte)(a.byteValue() / count);
            return c;
        }
        if (a instanceof Short) {
            final Short c2 = (short)(a.shortValue() / count);
            return c2;
        }
        if (a instanceof Integer) {
            final Integer c3 = (int)(a.intValue() / count);
            return c3;
        }
        if (a instanceof Long) {
            final Long c4 = a.longValue() / count;
            return c4;
        }
        if (a instanceof Float) {
            final Float c5 = a.floatValue() / count;
            return c5;
        }
        if (a instanceof Double) {
            final Double c6 = a.doubleValue() / count;
            return c6;
        }
        return 0;
    }
    
    private void createStatsData(final Map<String, Object> groupByData, final Map<String, Object> data, final StatsType type) {
        Map<String, Object> groupByStatsData = (Map<String, Object>)groupByData.get(type.name());
        if (groupByStatsData == null) {
            groupByStatsData = new HashMap<String, Object>();
            groupByData.put(type.name(), groupByStatsData);
        }
        final Map<String, Object> statsData = (Map<String, Object>)data.get(type.name());
        Iterator<Map.Entry<String, Object>> iterator = null;
        if (statsData != null) {
            iterator = statsData.entrySet().iterator();
        }
        else {
            iterator = data.entrySet().iterator();
        }
        while (iterator.hasNext()) {
            final Map.Entry<String, Object> dataEntry = iterator.next();
            if (HQuery.dontMergeSet.contains(dataEntry.getKey())) {
                continue;
            }
            final Object currGroupByValue = groupByStatsData.get(dataEntry.getKey());
            final Object currValue = dataEntry.getValue();
            if (!(currValue instanceof Number)) {
                continue;
            }
            switch (type) {
                case min: {
                    if (currGroupByValue == null || ((Comparable)currValue).compareTo(currGroupByValue) < 0) {
                        groupByStatsData.put(dataEntry.getKey(), currValue);
                        continue;
                    }
                    continue;
                }
                case max: {
                    if (currGroupByValue == null || ((Comparable)currValue).compareTo(currGroupByValue) > 0) {
                        groupByStatsData.put(dataEntry.getKey(), currValue);
                        continue;
                    }
                    continue;
                }
                case sum: {
                    groupByStatsData.put(dataEntry.getKey(), this.sum((Number)currValue, (Number)currGroupByValue));
                    continue;
                }
                case ave: {
                    final Map<String, Object> groupBySumData = (Map<String, Object>)groupByData.get(StatsType.sum.name());
                    final Object sumValue = groupBySumData.get(dataEntry.getKey());
                    final Long count = (Long)groupByData.get("count");
                    groupByStatsData.put(dataEntry.getKey(), this.ave((Number)sumValue, count));
                    continue;
                }
            }
        }
    }
    
    private void mergeSampleData(final Map<String, Object> groupByData, final Map<String, Object> data) {
        final Long count = (Long)groupByData.get("count");
        Long dataCount = (Long)data.get("count");
        if (dataCount == null) {
            dataCount = 1L;
        }
        groupByData.put("count", (count == null) ? ((long)dataCount) : (count + dataCount));
        final Long dataTs = (Long)data.get("timestamp");
        final Long groupByTs = (Long)groupByData.get("timestamp");
        if (groupByTs == null || dataTs > groupByTs) {
            groupByData.put("timestamp", dataTs);
            for (final Map.Entry<String, Object> dataEntry : data.entrySet()) {
                if (HQuery.dontMergeSet.contains(dataEntry.getKey())) {
                    continue;
                }
                groupByData.put(dataEntry.getKey(), dataEntry.getValue());
            }
        }
        this.createStatsData(groupByData, data, StatsType.min);
        this.createStatsData(groupByData, data, StatsType.max);
        this.createStatsData(groupByData, data, StatsType.sum);
        this.createStatsData(groupByData, data, StatsType.ave);
    }
    
    private void sampleResults(final Map<String, Object> data) {
        final String groupByValue = this.getGroupByValue(data);
        Map<Long, Map<String, Object>> groupByList = this.sampledResults.get(groupByValue);
        if (groupByList == null) {
            groupByList = new TreeMap<Long, Map<String, Object>>();
            this.sampledResults.put(groupByValue, groupByList);
        }
        final Long dataTs = (Long)data.get("timestamp");
        if (dataTs == null) {
            return;
        }
        final Long sampleTimePeriod = this.timePeriods.ceiling(dataTs);
        if (sampleTimePeriod == null) {
            return;
        }
        Map<String, Object> groupByData = groupByList.get(sampleTimePeriod);
        if (groupByData == null) {
            groupByData = new HashMap<String, Object>();
            groupByList.put(sampleTimePeriod, groupByData);
        }
        this.mergeSampleData(groupByData, data);
    }
    
    private void finalizeSampleResults() {
        if (this.sampledResults == null) {
            return;
        }
        for (final Map.Entry<String, Map<Long, Map<String, Object>>> sampleEntry : this.sampledResults.entrySet()) {
            final Object key = sampleEntry.getKey();
            for (final Map.Entry<Long, Map<String, Object>> sampleKeyEntry : sampleEntry.getValue().entrySet()) {
                final UUID id = new UUID((long)sampleKeyEntry.getKey());
                final HDKey wkey = new HDKey(id, key);
                this.finalResults.put(wkey, sampleKeyEntry.getValue());
            }
        }
    }
    
    private boolean processResult(final HDKey wkey, final Map<String, Object> data) {
        this.finalized = false;
        ++this.accepted;
        if (this.getLatestPerKey) {
            this.adjustLatestPerKey(wkey, data);
        }
        else {
            if (!this.useMaxResults) {
                this.finalResults.put(wkey, data);
                return true;
            }
            if (this.sampledResults == null) {
                if (this.globalStats == null) {
                    return false;
                }
                this.setGlobalStats(this.globalStats);
            }
            if (!this.doSampling) {
                this.timePeriods.add((Long)data.get("timestamp"));
            }
            this.sampleResults(data);
        }
        return false;
    }
    
    @Override
    public void runOne(final HD hd) {
        ++this.processed;
        if (!this.matches(hd)) {
            return;
        }
        final Map<String, Object> result = this.getHDAsMap(hd);
        this.processResult(hd.getMapKey(), result);
    }
    
    @Override
    public void mergeResults(final Map<HDKey, Map<String, Object>> results) {
        for (final Map.Entry<HDKey, Map<String, Object>> entry : results.entrySet()) {
            this.processResult(entry.getKey(), entry.getValue());
        }
    }
    
    @Override
    public Map<HDKey, Map<String, Object>> getResults() {
        if (!this.finalized) {
            if (this.getLatestPerKey || this.useMaxResults) {
                this.finalResults.clear();
            }
            if (this.getLatestPerKey) {
                this.finalizeLatestPerKey();
            }
            else if (this.useMaxResults) {
                this.finalizeSampleResults();
            }
            this.sort();
            this.limit();
            this.finalized = true;
        }
        return this.finalResults;
    }
    
    @Override
    public boolean usesSingleKey() {
        return this.singleKey != null;
    }
    
    @Override
    public HDKey getSingleKey() {
        return this.singleKey;
    }
    
    @Override
    public void setSingleKey(final HDKey key) {
        this.singleKey = key;
    }
    
    @Override
    public boolean usesPartitionKey() {
        return this.key != null;
    }
    
    @Override
    public Object getPartitionKey() {
        return this.key;
    }
    
    @Override
    public String toString() {
        String ret = "SELECT ";
        if (this.fields == null || this.fields.length == 0) {
            ret += "*";
        }
        else {
            boolean first = true;
            for (final String f : this.fields) {
                if (!first) {
                    ret += ", ";
                }
                ret += f;
                first = false;
            }
        }
        if (this.getLatestPerKey) {
            ret += " LATEST";
        }
        ret = ret + " FROM " + this.wsName;
        ret = ret + " " + this.getWhere(null, false);
        if (this.sortBy != null) {
            ret = ret + " SORT BY " + this.sortBy + " " + (this.sortDescending ? "DESC" : "ASC");
        }
        if (this.limit != -1) {
            ret = ret + " LIMIT " + this.limit;
        }
        if (this.useMaxResults) {
            ret = ret + " SAMPLE TO " + this.maxResults;
        }
        return ret;
    }
    
    public String getWhere(String qualifier, final boolean forNative) {
        String where = null;
        if (qualifier == null) {
            qualifier = "";
        }
        else {
            qualifier += ".";
        }
        if (this.key != null) {
            if (forNative) {
                where = qualifier + "partitionKey = '" + this.key + "'";
            }
            else {
                where = qualifier + "key = '" + this.key + "'";
            }
        }
        if (this.startTime != null) {
            if (where != null) {
                where += " AND ";
            }
            else {
                where = "";
            }
            where = where + qualifier + "hdTs >= " + this.startTime + "";
        }
        if (this.endTime != null) {
            if (where != null) {
                where += " AND ";
            }
            else {
                where = "";
            }
            where = where + qualifier + "hdTs <= " + this.endTime + "";
        }
        if (this.contextFilter != null && !this.contextFilter.isEmpty()) {
            if (where != null) {
                where += " AND ";
            }
            else {
                where = "";
            }
            boolean first = true;
            for (final Map.Entry<String, FilterCondition> entry : this.contextFilter.entrySet()) {
                if (!first) {
                    where += " AND ";
                }
                where = where + qualifier + entry.getKey() + " " + entry.getValue();
                first = false;
            }
        }
        return (where != null) ? ("WHERE " + where) : "";
    }
    
    public static void main(final String[] args) {
        final String[] conditions = { "$IN$one~two~three", "$IN$1~2~3", "$BTWN$10~20", "$BTWN$95000~96000", "$LIKE$abc", "$GT$10", "$GTE$10", "$LT$10", "$LTE$10", "$EQ$10", "$EQ$abc", "$NE$10", "$NE$10", "$NE$abc" };
        final Class<?>[] classes = (Class<?>[])new Class[] { String.class, Integer.class, Integer.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, String.class, Integer.class, String.class, String.class };
        final Object[][] testValues = { { "one", "two", "four", 2 }, { 1, 2, 4, "two" }, { 1, 10, 15, 30, "forty" }, { "94086", "95051", "96000", "99000", 20 }, { "abc", "gabch", "GABCH", 4 }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "abc", "def" }, { 1, 10, 20, "10", "def" }, { 1, 10, 20, "abc", "def" } };
        for (int i = 0; i < conditions.length; ++i) {
            final String condition = conditions[i];
            final Class<?> clazz = classes[i];
            try {
                final FilterCondition fc = new FilterCondition(condition, clazz);
                final Object[] array;
                final Object[] objs = array = testValues[i];
                for (final Object obj : array) {
                    try {
                        System.out.println(fc + " on " + obj + " = " + fc.evaluate(obj));
                    }
                    catch (Exception e1) {
                        System.out.println("* Could not run condition " + fc + " on " + obj + ": " + e1);
                    }
                }
            }
            catch (Exception e2) {
                System.out.println("* Could not create condition " + condition + ": " + e2);
            }
        }
    }
    
    @Override
    public List<HD> executeQuery(final Queryable<HDKey, HD> cache) {
        final Collection<HD> hds = this.getHDs(cache);
        final List<HD> res = new ArrayList<HD>();
        this.addQueryResults(hds, res);
        return res;
    }
    
    @Override
    public void addQueryResults(final Iterable<HD> hds, final Collection<HD> res) {
        if (hds != null) {
            for (final HD hd : hds) {
                ++this.processed;
                if (this.matches(hd)) {
                    res.add(hd);
                }
            }
        }
    }
    
    @Override
    public List<HD> getQueryResults() {
        return this.simpleQueryResults;
    }
    
    @Override
    public void mergeQueryResults(final List<HD> results) {
        this.simpleQueryResults.addAll(results);
    }
    
    static {
        HQuery.logger = Logger.getLogger((Class)HQuery.class);
        (HQuery.dontMergeSet = new HashSet<String>()).add("count");
        HQuery.dontMergeSet.add("timestamp");
        HQuery.dontMergeSet.add("min");
        HQuery.dontMergeSet.add("max");
        HQuery.dontMergeSet.add("ave");
        HQuery.dontMergeSet.add("sum");
        HQuery.dontMergeSet.add("eventList");
    }
    
    public enum OpType
    {
        IN, 
        BTWN, 
        LIKE, 
        GT, 
        LT, 
        GTE, 
        LTE, 
        EQ, 
        NE;
    }
    
    public static class FilterCondition implements Serializable
    {
        private static final long serialVersionUID = -757349114310144139L;
        OpType opType;
        Set<Object> inData;
        Object comp1;
        Object comp2;
        
        public FilterCondition(final String condition, final Class<?> valueClass) {
            this.inData = new HashSet<Object>();
            final OpType[] values = OpType.values();
            int i = 0;
            while (i < values.length) {
                final OpType ot = values[i];
                final String id = "$" + ot + "$";
                if (condition.startsWith(id)) {
                    if (condition.length() == id.length()) {
                        throw new RuntimeException("Condition type " + ot + " requires parameters in " + condition);
                    }
                    final String params = condition.substring(id.length());
                    final String[] paramSplit = params.split("[~]");
                    Object[] objs = null;
                    Label_0272: {
                        if (!String.class.equals(valueClass)) {
                            objs = new Object[paramSplit.length];
                            try {
                                final Constructor c = valueClass.getConstructor(String.class);
                                for (int j = 0; j < paramSplit.length; ++j) {
                                    objs[j] = c.newInstance(paramSplit[j]);
                                }
                                break Label_0272;
                            }
                            catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex2) {
                                throw new RuntimeException("Cannot generate condition " + condition + " on " + valueClass.getSimpleName() + ":" + ex2.getMessage());
                            }
                        }
                        objs = paramSplit;
                    }
                    this.opType = ot;
                    this.setData(objs);
                    break;
                }
                else {
                    ++i;
                }
            }
            if (this.opType == null) {
                this.opType = OpType.EQ;
                this.setData(new Object[] { condition });
            }
        }
        
        public void setData(final Object[] data) {
            switch (this.opType) {
                case IN: {
                    if (data.length == 0) {
                        throw new RuntimeException("IN operator needs 1 or more parameter");
                    }
                    for (final Object o : data) {
                        this.inData.add(o);
                    }
                    break;
                }
                case BTWN: {
                    if (data.length != 2) {
                        throw new RuntimeException("BTWN operator needs 2 parameters");
                    }
                    this.comp1 = data[0];
                    this.comp2 = data[1];
                    break;
                }
                default: {
                    if (data.length != 1) {
                        throw new RuntimeException(this + " operator needs 1 parameter");
                    }
                    this.comp1 = data[0];
                    break;
                }
            }
        }
        
        public boolean evaluate(final Object val) {
            if (val == null) {
                return false;
            }
            switch (this.opType) {
                case IN: {
                    return this.inData.contains(val);
                }
                case BTWN: {
                    return val instanceof Comparable && ((Comparable)val).compareTo(this.comp1) >= 0 && ((Comparable)val).compareTo(this.comp2) <= 0;
                }
                case LIKE: {
                    return val.toString().toLowerCase().contains(this.comp1.toString().toLowerCase());
                }
                case GT: {
                    return val instanceof Comparable && ((Comparable)val).compareTo(this.comp1) > 0;
                }
                case GTE: {
                    return val instanceof Comparable && ((Comparable)val).compareTo(this.comp1) >= 0;
                }
                case LT: {
                    return val instanceof Comparable && ((Comparable)val).compareTo(this.comp1) < 0;
                }
                case LTE: {
                    return val instanceof Comparable && ((Comparable)val).compareTo(this.comp1) <= 0;
                }
                case EQ: {
                    return val.equals(this.comp1);
                }
                case NE: {
                    return !val.equals(this.comp1);
                }
                default: {
                    return false;
                }
            }
        }
        
        public String quoteIfString(final Object obj) {
            if (obj instanceof String) {
                return "'" + obj + "'";
            }
            return obj.toString();
        }
        
        @Override
        public String toString() {
            switch (this.opType) {
                case BTWN: {
                    return "BETWEEN " + this.quoteIfString(this.comp1) + " AND " + this.quoteIfString(this.comp2);
                }
                case EQ: {
                    return "= " + this.quoteIfString(this.comp1);
                }
                case GT: {
                    return "> " + this.quoteIfString(this.comp1);
                }
                case GTE: {
                    return ">= " + this.quoteIfString(this.comp1);
                }
                case IN: {
                    String in = "IN (";
                    boolean first = true;
                    for (final Object inDatum : this.inData) {
                        if (!first) {
                            in += ", ";
                        }
                        else {
                            first = false;
                        }
                        in += this.quoteIfString(inDatum);
                    }
                    in += ")";
                    return in;
                }
                case LIKE: {
                    return "LIKE '%" + this.comp1 + "%'";
                }
                case LT: {
                    return "< " + this.quoteIfString(this.comp1);
                }
                case LTE: {
                    return "<= " + this.quoteIfString(this.comp1);
                }
                case NE: {
                    return "<> " + this.quoteIfString(this.comp1);
                }
                default: {
                    return null;
                }
            }
        }
    }
    
    public class EventsComparator implements Comparator<SimpleEvent>
    {
        @Override
        public int compare(final SimpleEvent o1, final SimpleEvent o2) {
            double n1 = 0.0;
            double n2 = 0.0;
            n1 = o1.timeStamp;
            n2 = o2.timeStamp;
            if (n1 > n2) {
                return 1;
            }
            if (n1 < n2) {
                return -1;
            }
            return o1._da_SimpleEvent_ID.compareTo(o2._da_SimpleEvent_ID);
        }
    }
    
    public static class ResultMapComparator implements Comparator<HDKey>, Serializable
    {
        private static final long serialVersionUID = 3127965059367797612L;
        String mapField;
        transient Map<HDKey, Map<String, Object>> base;
        boolean descending;
        
        public ResultMapComparator() {
        }
        
        public ResultMapComparator(final String mapField, final boolean descending) {
            this.mapField = mapField;
            this.descending = descending;
        }
        
        @Override
        public int compare(final HDKey o1, final HDKey o2) {
            final int c = this.descending ? -1 : 1;
            if (this.base == null) {
                return c * o1.compareTo(o2);
            }
            final Map<String, Object> res1 = this.base.get(o1);
            final Map<String, Object> res2 = this.base.get(o2);
            if (res1 == null && res2 == null) {
                return o1.compareTo(o2);
            }
            if (res1 == null && res2 != null) {
                return c;
            }
            if (res1 != null && res2 == null) {
                return -c;
            }
            final Object ro1 = res1.get(this.mapField);
            final Object ro2 = res2.get(this.mapField);
            if (ro1 == null && ro2 == null) {
                return o1.compareTo(o2);
            }
            if (ro1 == null && ro2 != null) {
                return c;
            }
            if (ro1 != null && ro2 == null) {
                return -c;
            }
            if (!ro1.getClass().isInstance(ro2)) {
                return o1.compareTo(o2);
            }
            if (ro1 instanceof Number) {
                final double d1 = ((Number)ro1).doubleValue();
                final double d2 = ((Number)ro2).doubleValue();
                if (d1 > d2) {
                    return c;
                }
                if (d2 > d1) {
                    return -c;
                }
                return o1.compareTo(o2);
            }
            else {
                if (ro1 instanceof DateTime) {
                    final DateTime d3 = (DateTime)ro1;
                    final DateTime d4 = (DateTime)ro2;
                    final int ct = d3.compareTo((ReadableInstant)d4) * c;
                    return (ct == 0) ? o1.compareTo(o2) : ct;
                }
                if (ro1 instanceof String) {
                    final int ct2 = ((String)ro1).compareTo((String)ro2) * c;
                    return (ct2 == 0) ? o1.compareTo(o2) : ct2;
                }
                final String s1 = ro1.toString();
                final String s2 = ro2.toString();
                final int ct = s1.compareTo(s2);
                return (ct == 0) ? o1.compareTo(o2) : ct;
            }
        }
        
        public void setBaseMap(final Map<HDKey, Map<String, Object>> map) {
            this.base = map;
        }
        
        @Override
        public String toString() {
            return "Map Comparator for field " + this.mapField + " desc " + this.descending + " base " + (this.base != null);
        }
    }
    
    private enum StatsType
    {
        min, 
        max, 
        sum, 
        ave;
    }
}
