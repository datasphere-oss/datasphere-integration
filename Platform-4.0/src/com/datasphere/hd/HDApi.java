package com.datasphere.hd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.datasphere.anno.EntryPoint;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.event.QueryResultEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.persistence.HStore;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitoringApiQueryHandler;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class HDApi
{
    private static Logger logger;
    public static final String MONITOR_NAME = "WAStatistics";
    private static HDApi instance;
    private QueryValidator qVal;
    private QueryRunnerForHDApi forES;
    
    public static HDApi get() {
        return HDApi.instance;
    }
    
    private HDApi() {
        this.qVal = new QueryValidator(Server.server);
    }
    
    public String toJSON(final Object o) throws JsonProcessingException {
        final ObjectMapper mapper = ObjectMapperFactory.newInstance();
        final String result = mapper.writeValueAsString(o);
        return result;
    }
    
    public QueryValidator getqVal() {
        return this.qVal;
    }
    
    public void setqVal(final QueryValidator qVal) {
        this.qVal = qVal;
    }
    
    public QueryRunnerForHDApi getForES() {
        return this.forES;
    }
    
    public void setForES(final QueryRunnerForHDApi forES) {
        this.forES = forES;
    }
    
    public List<MetaInfo.Type>[] getHDStoreDef(final String storename) throws Exception {
        return HStore.getHDStoreDef(storename);
    }
    
    public Object get(final String storename, final HDKey key, final String[] fields, final Map<String, Object> filter) throws Exception {
        return this.get(storename, key, fields, filter, HSecurityManager.TOKEN);
    }
    
    public Object get(final String storename, final HDKey key, final String[] fields, Map<String, Object> filter, final AuthToken token) throws Exception {
        final HazelcastInstance hz = HazelcastSingleton.get();
        if (filter == null) {
            filter = new HashMap<String, Object>();
        }
        if (!filter.containsKey("singlehds")) {
            filter.put("singlehds", "false");
        }
        RemoteCall handler = null;
        if (storename.equals("WAStatistics")) {
            handler = new MonitoringApiQueryHandler(storename, key, fields, filter, token, true);
        }
        else {
            handler = new HDApiQueryHandler(storename, key, fields, filter, token);
        }
        Object results = null;
        if (HazelcastSingleton.isClientMember()) {
            final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
            if (srvs.isEmpty()) {
                throw new RuntimeException("No available Server to get hds data");
            }
            final Object tR = DistributedExecutionManager.exec(hz, (Callable<Object>)handler, srvs);
            if (tR instanceof Collection) {
                final Collection coll = (Collection)tR;
                if (coll.size() != 1) {
                    throw new RuntimeException("Expected exactly 1 map(Map<HDKey, Map<String, Object>>) with All HDs. But got " + coll.size());
                }
                results = coll.iterator().next();
            }
        }
        else {
            results = handler.call();
        }
        handler = null;
        return results;
    }
    
    public Object executeQuery(final String storename, final String queryString, final Map<String, Object> queryParams, final AuthToken token) throws Exception {
        final HazelcastInstance hz = HazelcastSingleton.get();
        HDApiQueryHandler handler = new HDApiQueryHandler(storename, queryString, queryParams, token);
        Object results = null;
        if (HazelcastSingleton.isClientMember()) {
            final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
            if (srvs.isEmpty()) {
                throw new RuntimeException("No available Server to get hds data");
            }
            final Object tR = DistributedExecutionManager.exec(hz, (Callable<Object>)handler, srvs);
            if (tR instanceof Collection) {
                final Collection coll = (Collection)tR;
                if (coll.size() != 1) {
                    throw new RuntimeException("Expected exactly 1 map(Map<HDKey, Map<String, Object>>) with All HDs. But got " + coll.size());
                }
                results = coll.iterator().next();
            }
        }
        else {
            results = handler.call();
        }
        handler = null;
        return results;
    }
    
    public static Object covertTaskEventsToMap(final ITaskEvent taskEvent, final String keyField, final String[] fields, final Map<String, String> ctxFields, final boolean isSqlContext) {
        final long stime = System.currentTimeMillis();
        final Iterator<DARecord> eventIterator = (Iterator<DARecord>)taskEvent.batch().iterator();
        final List<Map<String, Object>> mMap = new ArrayList<Map<String, Object>>();
        final StringBuilder builder = new StringBuilder();
        String[] fieldInfo = null;
        while (eventIterator.hasNext()) {
            final HDKey wKey = new HDKey();
            final Map<String, Object> data = new HashMap<String, Object>();
            final QueryResultEvent qE = (QueryResultEvent)eventIterator.next().data;
            final String[] fieldsInfo = qE.getFieldsInfo();
            if (isSqlContext) {
                wKey.setId(qE.getIDString());
                wKey.setHDKey(qE.getKey());
                for (int ii = 0; ii < fieldsInfo.length; ++ii) {
                    data.put(fieldsInfo[ii], qE.getPayload()[ii]);
                }
            }
            else if (fieldsInfo[0].startsWith("last")) {
                if (qE.getPayload()[0] instanceof JsonNodeEvent) {
                    final JsonNodeEvent nodeEvent = (JsonNodeEvent)qE.getPayload()[0];
                    final JsonNode node = nodeEvent.getData();
                    builder.setLength(0);
                    for (final Map.Entry<String, String> entry : ctxFields.entrySet()) {
                        final JsonNode val = node.get((String)entry.getKey());
                        if (val != null) {
                            if (val instanceof TextNode) {
                                data.put(builder.append(entry.getKey()).toString(), val.textValue());
                            }
                            else {
                                data.put(builder.append(entry.getKey()).toString(), val.toString());
                            }
                            builder.setLength(0);
                        }
                    }
                }
                else {
                    if (fieldInfo == null) {
                        fieldInfo = extractFieldName(qE.getFieldsInfo());
                    }
                    builder.setLength(0);
                    for (int ii = 0; ii < fieldInfo.length; ++ii) {
                        data.put(builder.append(fieldInfo[ii]).toString(), qE.getPayload()[ii]);
                        builder.setLength(0);
                    }
                }
            }
            else {
                builder.setLength(0);
                for (int ii = 0; ii < fieldsInfo.length; ++ii) {
                    data.put(builder.append(qE.getFieldsInfo()[ii]).toString(), qE.getPayload()[ii]);
                    builder.setLength(0);
                }
            }
            mMap.add(data);
        }
        if (HDApi.logger.isDebugEnabled()) {
            HDApi.logger.debug((Object)("TaskEvent size: " + taskEvent.batch().size()));
            HDApi.logger.debug((Object)("Result Map size: " + mMap.size()));
        }
        final long etime = System.currentTimeMillis();
        HDApi.logger.info((Object)("Time to convert data to consumable format: " + (etime - stime) / 1000.0 + " seconds"));
        return mMap;
    }
    
    private static String[] extractFieldName(final String[] fieldsInfo) {
        final String[] fields = new String[fieldsInfo.length];
        int ii = 0;
        for (final String ss : fieldsInfo) {
            final int dot = ss.indexOf(".") + 1;
            final int cBrac = ss.indexOf(")");
            final String actualField = ss.substring(dot, cBrac);
            fields[ii] = actualField;
            ++ii;
        }
        return fields;
    }
    
    public static ITaskEvent convertApiResultToEvents(final Map<HDKey, Map<String, Object>> map) {
        final LinkedList<DARecord> linkedList = new LinkedList<DARecord>();
        for (final Map.Entry<HDKey, Map<String, Object>> entry : map.entrySet()) {
            final Map<String, Object> entryVal = entry.getValue();
            final QueryResultEvent qE = new QueryResultEvent();
            final String timestamp = (String)entryVal.get("timestamp");
            qE.setID(new UUID(timestamp));
            qE.setTimeStamp((long)new Long(timestamp));
            final List<String> allFields = new ArrayList<String>();
            final List<Object> payload = new ArrayList<Object>();
            for (final Map.Entry<String, Object> entries : entryVal.entrySet()) {
                if (entries.getKey().startsWith("context-")) {
                    final String[] fieldInfo = entries.getKey().split("-");
                    allFields.add(fieldInfo[1]);
                    payload.add(entries.getValue());
                }
            }
            qE.setFieldsInfo(allFields.toArray(new String[allFields.size()]));
            qE.setPayload(payload.toArray(new Object[payload.size()]));
            final DARecord DARecord = new DARecord();
            DARecord.data = qE;
            linkedList.add(DARecord);
        }
        final ITaskEvent TE = (ITaskEvent)TaskEvent.createStreamEvent(linkedList);
        return TE;
    }
    
    @EntryPoint(usedBy = 1)
    public Object getAllHDs(final String storename, final String[] fields, final Map<String, Object> filter, final AuthToken authToken) throws Exception {
        if (storename.equals("WAStatistics")) {
            final MonitoringApiQueryHandler handler = new MonitoringApiQueryHandler(storename, null, fields, filter, authToken, true);
            return handler.get();
        }
        return this.get(storename, null, fields, filter, authToken);
    }
    
    @EntryPoint(usedBy = 1)
    public Object getHD(final String storename, final HDKey key, final String[] fields, final Map<String, Object> filter, final AuthToken authToken) throws Exception {
        if (storename.equals("WAStatistics")) {
            final MonitoringApiQueryHandler handler = new MonitoringApiQueryHandler(storename, key, fields, filter, authToken, false);
            return handler.get();
        }
        return this.get(storename, key, fields, filter, authToken);
    }
    
    static {
        HDApi.logger = Logger.getLogger((Class)HDApi.class);
        HDApi.instance = new HDApi();
    }
    
    public enum HDKeyFields
    {
        uuid {
            @Override
            public String getFieldRepresentation() {
                return "$id";
            }
        }, 
        key {
            @Override
            public String getFieldRepresentation() {
                return "";
            }
        }, 
        timestamp {
            @Override
            public String getFieldRepresentation() {
                return "$timestamp";
            }
        };
        
        public abstract String getFieldRepresentation();
    }
}
