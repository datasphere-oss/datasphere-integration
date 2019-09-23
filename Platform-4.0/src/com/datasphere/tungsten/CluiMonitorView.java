package com.datasphere.tungsten;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.datasphere.appmanager.AppManagerRequestClient;
import com.datasphere.appmanager.ApplicationStatusResponse;
import com.datasphere.appmanager.ChangeApplicationStateResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonSyntaxException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDClientOps;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorModel;
import com.datasphere.runtime.monitor.MonitoringApiQueryHandler;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HDKey;

public class CluiMonitorView
{
    private static Logger logger;
    public static final SimpleDateFormat DATE_FORMAT_MILLIS;
    private static final String DATE_FORMAT_REGEX_MILLIS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d";
    public static final SimpleDateFormat DATE_FORMAT_SECS;
    private static final String DATE_FORMAT_REGEX_SECS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d";
    public static final SimpleDateFormat DATE_FORMAT_MINS;
    private static final String DATE_FORMAT_REGEX_MINS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d";
    private static final String DATE_FORMAT_REGEX_HOUR_MIN = "\\d\\d:\\d\\d";
    private static final String DATE_FORMAT_REGEX_ALL_DIGITS = "\\d\\d:\\d\\d";
    private static final String TIME_SERIES_REPORT = "TimeSeriesReport";
    private static final String SUMMARY_REPORT = "SummaryReport";
    private static final Set<String> DEFINED_REPORTS;
    public static final int TABLE_WIDTH = 135;
    private static final long FIVE_SECONDS = 5000L;
    private static final String appTableFormat = "= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n";
    private static final String singleCompFormat = "= %-50s%-82s=%n";
    private static final String serverTableFormat = "= %-42s%-40s%-20s%-10s%-20s=%n";
    private static final String kafkaTableFormat = "= %-42s%-90s=%n";
    private static final int WORD_WRAP_MAX = 10;
    static boolean detailView;
    
    private static MetaInfo.MetaObject getObjectByName(final String name) {
        for (final EntityType aType : EntityType.values()) {
            if (aType.equals(EntityType.SERVER)) {
                try {
                    return MetadataRepository.getINSTANCE().getServer(name, Tungsten.session_id);
                }
                catch (MetaDataRepositoryException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            String fullName;
            if (name.indexOf(46) != -1) {
                fullName = name;
            }
            else if (aType.isGlobal()) {
                fullName = "Global." + name;
            }
            else {
                fullName = Tungsten.ctx.addSchemaPrefix(null, name);
            }
            final String[] namespaceAndName = fullName.split("\\.");
            try {
                final MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByName(aType, namespaceAndName[0], namespaceAndName[1], null, Tungsten.session_id);
                if (obj != null) {
                    return obj;
                }
            }
            catch (MetaDataRepositoryException e2) {
                e2.printStackTrace();
            }
        }
        return null;
    }
    
    public static void handleMonitorRequest(final List<String> tokens) throws MetaDataRepositoryException {
        if (!MonitorModel.monitorIsEnabled()) {
            System.out.println("Monitor is disabled");
            return;
        }
        if (tokens.size() == 1 && tokens.get(0).equalsIgnoreCase("help")) {
            printUsage();
            return;
        }
        MetaInfo.MetaObject entityInfo = null;
        MetaInfo.Server serverInfo = null;
        boolean verbose = false;
        long followTime = -1L;
        Long startTime = null;
        Long endTime = null;
        String format = null;
        String types = null;
        String export = null;
        for (int tokenIndex = 0; tokenIndex < tokens.size(); ++tokenIndex) {
            String token = tokens.get(tokenIndex);
            final int nextIndex = tokenIndex + 1;
            if (token != null) {
                if (token.equals("-v")) {
                    verbose = true;
                }
                else if (token.equalsIgnoreCase("all")) {
                    CluiMonitorView.detailView = true;
                }
                else if (token.equalsIgnoreCase("-follow")) {
                    if (nextIndex < tokens.size()) {
                        token = tokens.get(nextIndex);
                        try {
                            followTime = Integer.valueOf(token) * 1000;
                            tokenIndex = nextIndex;
                        }
                        catch (NumberFormatException e) {
                            followTime = 5000L;
                        }
                    }
                    else {
                        followTime = 5000L;
                    }
                }
                else if (token.equalsIgnoreCase("-start")) {
                    if (nextIndex >= tokens.size()) {
                        System.out.println("MONITOR -start requires an argument");
                        return;
                    }
                    token = tokens.get(nextIndex);
                    startTime = convertToTimestamp(token);
                    if (startTime == null) {
                        System.out.println("MONITOR -start requires an argument in the format " + CluiMonitorView.DATE_FORMAT_MINS.toPattern());
                        return;
                    }
                    tokenIndex = nextIndex;
                }
                else if (token.equalsIgnoreCase("-end")) {
                    if (nextIndex >= tokens.size()) {
                        System.out.println("MONITOR -end requires an argument");
                        return;
                    }
                    token = tokens.get(nextIndex);
                    endTime = convertToTimestamp(token);
                    if (endTime == null) {
                        System.out.println("MONITOR -end requires an argument in the format " + CluiMonitorView.DATE_FORMAT_MINS.toPattern());
                        return;
                    }
                    tokenIndex = nextIndex;
                }
                else if (token.equalsIgnoreCase("-format")) {
                    if (nextIndex >= tokens.size()) {
                        System.out.println("MONITOR -format requires an argument");
                        return;
                    }
                    final String candidate = tokens.get(nextIndex);
                    boolean found = false;
                    for (final String definedReport : CluiMonitorView.DEFINED_REPORTS) {
                        if (definedReport.equalsIgnoreCase(candidate)) {
                            tokenIndex = nextIndex;
                            format = candidate;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        System.out.println("MONITOR -format value is invalid");
                        return;
                    }
                }
                else if (token.equalsIgnoreCase("-types")) {
                    if (nextIndex >= tokens.size()) {
                        System.out.println("MONITOR -types requires an argument");
                        return;
                    }
                    types = tokens.get(nextIndex);
                    tokenIndex = nextIndex;
                }
                else if (token.equalsIgnoreCase("-export")) {
                    if (nextIndex >= tokens.size()) {
                        System.out.println("MONITOR -export requires an argument");
                        return;
                    }
                    export = tokens.get(nextIndex);
                    tokenIndex = nextIndex;
                }
                else if (entityInfo == null) {
                    final MetaInfo.MetaObject mo = getObjectByName(token);
                    if (mo == null) {
                        System.out.println("MONITOR could not find application, node or component named " + token);
                        return;
                    }
                    entityInfo = mo;
                }
                else {
                    if (serverInfo != null) {
                        System.out.println("MONITOR syntax error reading " + token);
                        printUsage();
                        return;
                    }
                    final MetaInfo.MetaObject mo = getObjectByName(token);
                    if (!(mo instanceof MetaInfo.Server)) {
                        System.out.println("MONITOR could not find node named " + token);
                        return;
                    }
                    serverInfo = (MetaInfo.Server)mo;
                }
            }
        }
        executeCommand(entityInfo, serverInfo, verbose, followTime, startTime, endTime, format, types, export);
        if (CluiMonitorView.detailView) {
            CluiMonitorView.detailView = false;
        }
    }
    
    private static void basicMonitorStatus(final scala.collection.mutable.StringBuilder target, final boolean verbose, final Long startTime, final Long endTime) throws MetaDataRepositoryException {
        final MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
        Set<MetaInfo.MetaObject> mos = (Set<MetaInfo.MetaObject>)clientOps.getByEntityType(EntityType.APPLICATION, Tungsten.session_id);
        final List<MetaInfo.Flow> flowMOs = new ArrayList<MetaInfo.Flow>();
        for (final MetaInfo.MetaObject mo : mos) {
            if (mo instanceof MetaInfo.Flow) {
                flowMOs.add((MetaInfo.Flow)mo);
            }
        }
        applicationsBlock(target, flowMOs, null, "APPLICATIONS", verbose, startTime, endTime);
        mos = (Set<MetaInfo.MetaObject>)clientOps.getByEntityType(EntityType.SERVER, Tungsten.session_id);
        final List<MetaInfo.Server> serverMOs = new ArrayList<MetaInfo.Server>();
        for (final MetaInfo.MetaObject mo2 : mos) {
            if (mo2 instanceof MetaInfo.Server) {
                serverMOs.add((MetaInfo.Server)mo2);
            }
        }
        serversBlock(target, serverMOs, "NODES", verbose, startTime, endTime);
        kafkaBlock(target);
        elasticsearchBlock(target, MonitorModel.ES_SERVER_UUID, false);
    }
    
    private static void kafkaBlock(final scala.collection.mutable.StringBuilder target) {
        final String[] fields = new String[0];
        final Map<String, Object> filter = new HashMap<String, Object>();
        filter.put("serverInfo", MonitorModel.KAFKA_SERVER_UUID);
        final Map<String, Object> summary = getEntitySummary(MonitorModel.KAFKA_ENTITY_UUID, fields, filter);
        tableTitle(target, "KAFKA");
        if (summary == null || summary.isEmpty()) {
            tableMessage(target, "No Kafka-based persistent streams in use");
        }
        else {
            final Object[] columnNames = { "Name", "Values" };
            target.append(String.format("= %-42s%-90s=%n", columnNames));
            tableDivider(target);
            for (final String name : summary.keySet()) {
                final Object value = summary.get(name);
                final Object[] printVals = humanizeData(name, value);
                final Object[] lineData = { printVals[0], printVals[1] };
                singleLine(target, "= %-42s%-90s=%n", lineData, 0);
            }
        }
        tableBottom(target);
    }
    
    private static void elasticsearchBlock(final scala.collection.mutable.StringBuilder target, final UUID serverID, final boolean includeHotThreads) {
        final String[] fields = new String[0];
        final Map<String, Object> filter = new HashMap<String, Object>();
        filter.put("serverInfo", MonitorModel.ES_SERVER_UUID);
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
        if (srvs.isEmpty()) {
            throw new RuntimeException("No available Node to get Monitor data");
        }
        final HDKey wk = new HDKey(MonitorModel.ES_ENTITY_UUID, MonitorModel.ES_SERVER_UUID);
        final RemoteCall action = new MonitoringApiQueryHandler("WAStatistics", wk, fields, filter, null, false);
        tableTitle(target, "ELASTICSEARCH");
        try {
            final Collection<Map<HDKey, Map<String, Object>>> manyResults = DistributedExecutionManager.exec(hz, (Callable<Map<HDKey, Map<String, Object>>>)action, srvs);
            if (manyResults.size() != 1) {
                CluiMonitorView.logger.warn((Object)("Expected result from exactly one node, but received " + manyResults.size()));
            }
            final Map<HDKey, Map<String, Object>> x = manyResults.iterator().next();
            final Object[] columnNames = { "Name", "Values" };
            target.append(String.format("= %-42s%-90s=%n", columnNames));
            tableDivider(target);
            Object hotThreads = null;
            for (final HDKey key : x.keySet()) {
                final Map<String, Object> summary = x.get(key);
                for (final String name : summary.keySet()) {
                    if (name.equalsIgnoreCase("name")) {
                        continue;
                    }
                    if (name.equalsIgnoreCase("id")) {
                        continue;
                    }
                    final Object value = summary.get(name);
                    if (name.equalsIgnoreCase("es-hot-threads")) {
                        hotThreads = value;
                    }
                    else {
                        final Object[] printVals = humanizeData(name, value);
                        final Object[] lineData = { printVals[0], printVals[1] };
                        singleLine(target, "= %-42s%-90s=%n", lineData, 0);
                    }
                }
            }
            if (includeHotThreads && hotThreads != null) {
                tableMessage(target, "Hot Threads:");
                final String[] split;
                final String[] lines = split = hotThreads.toString().split("\n");
                for (final String line : split) {
                    tableMessage(target, line);
                }
            }
        }
        catch (Exception e) {
            System.out.println("Could not get Elasticsearch data: " + e.getMessage());
        }
        tableBottom(target);
    }
    
    private static void serverBlock(final scala.collection.mutable.StringBuilder target, final MetaInfo.Server serverInfo, final Long startTime, final Long endTime) throws MetaDataRepositoryException {
        final List<MetaInfo.Server> serverInfos = new ArrayList<MetaInfo.Server>();
        serverInfos.add(serverInfo);
        serversBlock(target, serverInfos, "NODE " + serverInfo.name, true, startTime, endTime);
        elasticsearchBlock(target, serverInfo.uuid, true);
    }
    
    private static void serversBlock(final scala.collection.mutable.StringBuilder target, final List<MetaInfo.Server> serverInfos, final String tableTitle, final boolean recurse, final Long startTime, final Long endTime) throws MetaDataRepositoryException {
        tableTitle(target, tableTitle);
        if (serverInfos.size() > 0) {
            final Object[] columnNames = { "Name", "Version", "Free Mem", "CPU%", "Uptime" };
            target.append(String.format("= %-42s%-40s%-20s%-10s%-20s=%n", columnNames));
            tableDivider(target);
            for (final MetaInfo.Server serverInfo : serverInfos) {
                serverTree(target, serverInfo, recurse, startTime, endTime, 0);
            }
        }
        else {
            tableMessage(target, "No available nodes");
        }
        tableBottom(target);
    }
    
    private static void serverTree(final scala.collection.mutable.StringBuilder target, final MetaInfo.MetaObject metaObject, final boolean recurse, final Long startTime, final Long endTime, final int depth) throws MetaDataRepositoryException {
        final String[] fields = new String[0];
        final Map<String, Object> filter = new HashMap<String, Object>();
        if (startTime != null) {
            filter.put("start-time", startTime);
        }
        if (endTime != null) {
            filter.put("end-time", endTime);
        }
        final Map<String, Object> values = getEntitySummary(metaObject.uuid, fields, filter);
        if (values == null) {
            System.out.println("= " + metaObject.name + " - data not ready yet");
            return;
        }
        final Object name = EntityType.SERVER.equals(metaObject.type) ? getOneValue("name", values, "?") : getOneValue("full-name", values, "?");
        final Object version = getOneValue("version", values, ".");
        final Object memFree = getOneValue("memory-free", values, ".");
        final Object uptime = getOneValue("uptime", values, ".");
        final String uptimeStr = (uptime instanceof Long) ? getUptimeString((long)uptime) : uptime.toString();
        final String versionStr = version.toString().replace("Version ", "").replace("1.0.0-SNAPSHOT ", "");
        final String freeMemStr = (memFree instanceof Long) ? getMemoryString((long)memFree) : memFree.toString();
        Object cpu = getOneValue("cpu-rate", values, ".");
        if (cpu instanceof Long) {
            cpu = MonitorModel.renderCpuPercent((long)cpu);
        }
        final Object[] lineData = { name, versionStr, freeMemStr, cpu, uptimeStr };
        singleLine(target, "= %-42s%-40s%-20s%-10s%-20s=%n", lineData, depth);
        final MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
        if (recurse) {
            if (metaObject instanceof MetaInfo.Flow) {
                final MetaInfo.Flow flowMo = (MetaInfo.Flow)metaObject;
                for (final EntityType et : flowMo.getObjectTypes()) {
                    for (final UUID u : flowMo.getObjects(et)) {
                        final MetaInfo.MetaObject childMO = clientOps.getMetaObjectByUUID(u, Tungsten.session_id);
                        serverTree(target, childMO, recurse, startTime, endTime, depth + 1);
                    }
                }
            }
            else if (metaObject instanceof MetaInfo.Server) {
                final MetaInfo.Server serverMo = (MetaInfo.Server)metaObject;
                final Map<String, List<MetaInfo.MetaObject>> m = serverMo.getCurrentObjects();
                for (final String keyStr : m.keySet()) {
                    final List<MetaInfo.MetaObject> mos = m.get(keyStr);
                    for (final MetaInfo.MetaObject childMO2 : mos) {
                        if (childMO2 instanceof MetaInfo.Flow) {
                            serverTree(target, childMO2, recurse, startTime, endTime, depth + 1);
                        }
                    }
                }
            }
        }
    }
    
    private static void applicationBlock(final scala.collection.mutable.StringBuilder target, final MetaInfo.Flow applicationInfo, final MetaInfo.Server serverInfo, final boolean verbose, final Long startTime, final Long endTime) throws MetaDataRepositoryException {
        final List<MetaInfo.Flow> flowInfos = new ArrayList<MetaInfo.Flow>();
        flowInfos.add(applicationInfo);
        applicationsBlock(target, flowInfos, serverInfo, EntityType.APPLICATION.equals(applicationInfo.type) ? "APPLICATION" : "FLOW", verbose, startTime, endTime);
    }
    
    private static void applicationsBlock(final scala.collection.mutable.StringBuilder target, List<MetaInfo.Flow> flowInfos, final MetaInfo.Server serverInfo, final String tableTitle, final boolean recurse, final Long startTime, final Long endTime) throws MetaDataRepositoryException {
        tableTitle(target, tableTitle);
        if (flowInfos.size() > 0) {
            final Object[] columnNames = { "Name", "Status", "Rate", "SourceRate", "CPU%", "Nodes", "Activity" };
            target.append(String.format("= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n", columnNames));
            tableDivider(target);
            if (flowInfos != null && !CluiMonitorView.detailView) {
                flowInfos = new LinkedList<MetaInfo.Flow>((Collection<? extends MetaInfo.Flow>)Utility.removeInternalApplications(new HashSet<MetaInfo.MetaObject>(flowInfos)));
            }
            for (final MetaInfo.MetaObject flowInfo : flowInfos) {
                applicationTree(target, flowInfo, serverInfo, recurse, startTime, endTime, 0);
            }
        }
        else {
            tableMessage(target, "No applications");
        }
        tableBottom(target);
    }
    
    private static void applicationTree(final scala.collection.mutable.StringBuilder target, final MetaInfo.MetaObject metaObject, final MetaInfo.Server serverInfo, final boolean recurse, final Long startTime, final Long endTime, final int depth) throws MetaDataRepositoryException {
        final String[] fields = new String[0];
        final Map<String, Object> filter = new HashMap<String, Object>();
        if (serverInfo != null) {
            filter.put("serverInfo", serverInfo.uuid);
        }
        if (startTime != null) {
            filter.put("start-time", startTime);
        }
        if (endTime != null) {
            filter.put("end-time", endTime);
        }
        final Map<String, Object> values = getEntitySummary(metaObject.uuid, fields, filter);
        if (values == null) {
            System.out.println("= " + metaObject.name + " - data not ready yet");
            return;
        }
        String name = getOneValue("full-name", values, "?").toString();
        if (name.length() > 42) {
            name = name.substring(0, 20) + "~" + name.substring(name.length() - 20, name.length());
        }
        Object status = getOneValue("status", values, "-");
        if (status != null && status instanceof String) {
            final String statval = (String)status;
            final int pos = statval.lastIndexOf(":");
            if (pos != -1) {
                status = statval.substring(pos + 1);
            }
        }
        else {
            status = "-";
        }
        final Object rate = getOneValue("rate", values, ".");
        final Object sourceRate = getOneValue("source-rate", values, "");
        Object cpu = getOneValue("cpu-rate", values, ".");
        if (cpu instanceof Long) {
            cpu = MonitorModel.renderCpuPercent((long)cpu);
        }
        Object nodes = getOneValue("num-servers", values, ".");
        if (nodes == null) {
            nodes = ".";
        }
        final Object activityVal = getOneValue("latest-activity", values, null);
        final String activity = (activityVal == null) ? "" : CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)activityVal));
        final Object[] lineData = { name, status, rate, sourceRate, cpu, nodes, activity };
        singleLine(target, "= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n", lineData, depth);
        final MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
        if (recurse) {
            if (metaObject instanceof MetaInfo.Flow) {
                final MetaInfo.Flow flowMo = (MetaInfo.Flow)metaObject;
                for (final EntityType et : flowMo.getObjectTypes()) {
                    for (final UUID u : flowMo.getObjects(et)) {
                        final MetaInfo.MetaObject childMO = clientOps.getMetaObjectByUUID(u, Tungsten.session_id);
                        applicationTree(target, childMO, serverInfo, recurse, startTime, endTime, depth + 1);
                    }
                }
            }
            else if (metaObject instanceof MetaInfo.Server) {
                final MetaInfo.Server serverMo = (MetaInfo.Server)metaObject;
                final Map<String, List<MetaInfo.MetaObject>> m = serverMo.getCurrentObjects();
                for (final String keyStr : m.keySet()) {
                    final List<MetaInfo.MetaObject> mos = m.get(keyStr);
                    for (final MetaInfo.MetaObject childMO2 : mos) {
                        applicationTree(target, childMO2, serverInfo, recurse, startTime, endTime, depth + 1);
                    }
                }
            }
        }
    }
    
    private static void componentBlock(final scala.collection.mutable.StringBuilder target, final MetaInfo.MetaObject componentInfo, final MetaInfo.Server serverInfo, final Long startTime, final Long endTime, final String types, final boolean isExportFlag) {
        final String[] fields = { "most-recent-data" };
        final Map<String, Object> filter = new HashMap<String, Object>();
        if (serverInfo != null) {
            filter.put("serverInfo", serverInfo.uuid);
        }
        if (startTime != null) {
            filter.put("start-time", startTime);
        }
        if (endTime != null) {
            filter.put("end-time", endTime);
        }
        if (types != null && !types.isEmpty()) {
            filter.put("most-recent-data-fields", types);
        }
        final Map<String, Object> values = getEntitySummary(componentInfo.uuid, fields, filter);
        if (values == null) {
            target.append("= " + componentInfo.name + " - data not ready yet");
            return;
        }
        tableTitle(target, componentInfo.getFullName());
        final Object[] columnNames = { "Property", "Value" };
        target.append(String.format("= %-50s%-82s=%n", columnNames));
        tableDivider(target);
        final Map<String, Object> mostRecent = (Map<String, Object>)values.get("most-recent-data");
        if (types == null || types.isEmpty() || Arrays.asList(types.toUpperCase().split(",")).contains("NODES")) {
            final Object nodes = getOneValue("num-servers", values, ".");
            if (nodes != null) {
                mostRecent.put("Nodes", nodes);
            }
        }
        if (mostRecent != null) {
            String comments = null;
            for (final Map.Entry<String, Object> entry : mostRecent.entrySet()) {
                if (!CluiMonitorView.detailView) {
                    if (entry.getKey().equals("lag-report")) {
                        continue;
                    }
                    if (entry.getKey().equals("lag-rate")) {
                        continue;
                    }
                }
                if (entry.getKey().equals("lag-report-json")) {
                    continue;
                }
                if (entry.getKey().equals("cpu-thread")) {
                    continue;
                }
                if (entry.getKey().equals("comments")) {
                    comments = entry.getValue().toString();
                }
                else {
                    if (entry.getKey().equals(MonitorEvent.Type.ADAPTER_DETAIL.toString().toLowerCase().replace("_", "-")) && !isExportFlag) {
                        continue;
                    }
                    if (!(entry.getValue() instanceof String)) {
                        final Object[] humanizedData = humanizeData(entry.getKey(), entry.getValue());
                        singleLine(target, "= %-50s%-82s=%n", humanizedData, 0);
                    }
                    else {
                        final String value = entry.getValue().toString();
                        if (entry.getKey().equals(MonitorEvent.Type.ADAPTER_BASIC.toString().toLowerCase().replace("_", "-")) || (entry.getKey().equals(MonitorEvent.Type.ADAPTER_DETAIL.toString().toLowerCase().replace("_", "-")) && isExportFlag)) {
                            final String errorMsg = "Error in processing json for event type " + entry.getKey();
                            JSONObject jObject;
                            try {
                                jObject = new JSONObject(value);
                            }
                            catch (JSONException e) {
                                CluiMonitorView.logger.error((Object)(errorMsg + ". Cause: " + e.getMessage()));
                                continue;
                            }
                            final Iterator<?> keys = (Iterator<?>)jObject.keys();
                            final JSONObject modifiedEventJson = new JSONObject();
                            while (keys.hasNext()) {
                                final String serverName = (String)keys.next();
                                try {
                                    final JSONObject eventJson = jObject.getJSONObject(serverName);
                                    final Iterator<?> eventKeys = (Iterator<?>)eventJson.keys();
                                    while (eventKeys.hasNext()) {
                                        final String eventType = (String)eventKeys.next();
                                        Utility.insertOrAppendValueInJSON(modifiedEventJson, eventType, "{ " + serverName + ": " + eventJson.getString(eventType) + " }");
                                    }
                                }
                                catch (JSONException e2) {
                                    CluiMonitorView.logger.error((Object)(errorMsg + ". Cause: " + e2.getMessage()));
                                }
                            }
                            final Iterator<?> eventKeys2 = (Iterator<?>)modifiedEventJson.keys();
                            while (eventKeys2.hasNext()) {
                                final String eventType2 = (String)eventKeys2.next();
                                try {
                                    final String eventValue = modifiedEventJson.getString(eventType2);
                                    printSingleLineOutputForString(eventType2, eventValue, target);
                                }
                                catch (JSONException e3) {
                                    CluiMonitorView.logger.error((Object)(errorMsg + ". Cause: " + e3.getMessage()));
                                }
                            }
                        }
                        else {
                            printSingleLineOutputForString(entry.getKey(), value, target);
                        }
                    }
                }
            }
            if (comments != null) {
                tableDivider(target);
                tableMessage(target, "Comments:");
                final String[] split;
                final String[] lines = split = comments.split("\n");
                for (final String line : split) {
                    tableMessage(target, line);
                }
            }
        }
        tableBottom(target);
    }
    
    private static void printSingleLineOutputForString(String key, String value, final scala.collection.mutable.StringBuilder target) {
        try {
            value = Utility.getPrettyJsonString(value);
            final String charToDelete = "{},\"";
            final String pattern = "[" + Pattern.quote(charToDelete) + "]";
            value = value.replaceAll(pattern, "").replaceAll("(?m)^[ \t]*\r?\n", "");
        }
        catch (JsonSyntaxException ex) {}
        final String[] split;
        final String[] lines = split = value.split("\n");
        for (String line : split) {
            line = line.replaceFirst("^  ", "");
            final Object[] humanizedData = humanizeData(key, line);
            singleLine(target, "= %-50s%-82s=%n", humanizedData, 0);
            key = " ";
        }
    }
    
    private static void summaryReport(final scala.collection.mutable.StringBuilder target, final MetaInfo.MetaObject componentInfo, final MetaInfo.Server serverInfo, final Long startTime, final Long endTime, String types) {
        final String[] fields = { "most-recent-data", "stats", "time-series" };
        final Map<String, Object> filter = new HashMap<String, Object>();
        if (serverInfo != null) {
            filter.put("serverInfo", serverInfo.uuid);
        }
        if (startTime != null) {
            filter.put("start-time", startTime);
        }
        if (endTime != null) {
            filter.put("end-time", endTime);
        }
        if (types == null || types.isEmpty()) {
            if (componentInfo.getType() == EntityType.APPLICATION) {
                types = "source-input,stream-full,source-rate,output,checkpoint-summary";
            }
            else if (componentInfo.getType() == EntityType.SOURCE) {
                types = "input,rate,cpu-rate";
            }
            else if (componentInfo.getType() == EntityType.TARGET) {
                types = "input,output,target-acked,cpu-rate";
            }
            else if (componentInfo.getType() == EntityType.STREAM) {
                types = "input,input-rate,stream-full,";
            }
            else if (componentInfo.getType() == EntityType.WINDOW) {
                types = "num-partitions,input,window-size";
            }
            else if (componentInfo.getType() == EntityType.CACHE) {
                types = "cache-size,local-hits,local-misses,remote-hits,remote-misses,cache-refresh,rate";
            }
            else if (componentInfo.getType() == EntityType.CQ) {
                types = "input,output,input-rate,output-rate";
            }
            else if (componentInfo.getType() == EntityType.HDSTORE) {
                types = "input-rate,hds-created";
            }
            else {
                types = "input,input-rate,output,output-rate";
            }
        }
        filter.put("stats-fields", types);
        filter.put("time-series-fields", "source-rate,input-rate,rate");
        final Map<String, Object> values = getEntitySummary(componentInfo.uuid, fields, filter);
        if (values == null) {
            System.out.println("= " + componentInfo.name + " - data not ready yet");
            return;
        }
        tableTitle(target, componentInfo.getFullName() + " Summary Report");
        final Object[] columnNames = { "Property", "Value" };
        target.append(String.format("= %-50s%-82s=%n", columnNames));
        tableDivider(target);
        final String clusterName = HazelcastSingleton.get().getName();
        singleLine(target, "= %-50s%-82s=%n", humanizeData("Cluster Name", clusterName), 0);
        final Object striimVersion = getOneValue("striim-version", values, null);
        if (striimVersion != null) {
            singleLine(target, "= %-50s%-82s=%n", humanizeData("Striim Version", striimVersion), 0);
        }
        final Object mdcVersion = getOneValue("mdc-version", values, null);
        if (mdcVersion != null) {
            singleLine(target, "= %-50s%-82s=%n", humanizeData("Metadata Repository Version", mdcVersion), 0);
        }
        if (startTime != null) {
            singleLine(target, "= %-50s%-82s=%n", humanizeData("start", startTime), 0);
        }
        if (startTime != null) {
            singleLine(target, "= %-50s%-82s=%n", humanizeData("end", endTime), 0);
        }
        final Object clusterSize = getOneValue("num-servers", values, "Unknown");
        singleLine(target, "= %-50s%-82s=%n", humanizeData("Cluster size", clusterSize), 0);
        final Map<String, Object> mosftRecent = (Map<String, Object>)values.get("most-recent-data");
        final Map<String, Object> timeSeriesData = (Map<String, Object>)values.get("time-series-data");
        tableDivider(target);
        if (timeSeriesData != null) {
            for (final Map.Entry<String, Object> entry : timeSeriesData.entrySet()) {
                final String key = entry.getKey();
                if (!"source-rate".equalsIgnoreCase(key) && !"input-rate".equalsIgnoreCase(key) && !"rate".equalsIgnoreCase(key)) {
                    continue;
                }
                final Object value = entry.getValue();
                if (!(value instanceof List)) {
                    continue;
                }
                final List listOfPairs = (List)value;
                for (int i = 0; i < listOfPairs.size(); ++i) {
                    final Object item = listOfPairs.get(i);
                    final Pair pair = (Pair)item;
                    final Long rateInt = (Long)pair.second;
                    if (rateInt > 0L) {
                        final Object[] humanizedData = humanizeData("first-event-time", pair.first);
                        singleLine(target, "= %-50s%-82s=%n", humanizedData, 0);
                        break;
                    }
                }
                for (int i = listOfPairs.size() - 1; i >= 0; --i) {
                    final Object item = listOfPairs.get(i);
                    final Pair pair = (Pair)item;
                    final Long rateInt = (Long)pair.second;
                    if (rateInt > 0L) {
                        final Object[] humanizedData = humanizeData("last-event-time", pair.first);
                        singleLine(target, "= %-50s%-82s=%n", humanizedData, 0);
                        break;
                    }
                }
                break;
            }
        }
        final Map<String, Object> statsData = (Map<String, Object>)values.get("stats");
        if (statsData == null) {
            tableMessage(target, "No data found for that query");
        }
        else if (componentInfo.getType() == EntityType.APPLICATION) {
            if (statsData.containsKey("source-input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Source Input Diff", statsData.get("source-input").diff }, 0);
            }
            final String checkpointSummary = Tungsten.getCheckpointSummary(componentInfo.getUuid());
            if (checkpointSummary != null && checkpointSummary.length() > 0) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Checkpoint Summary", checkpointSummary }, 0);
            }
            if (statsData.containsKey("stream-full")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Backpressure %", asPercent(statsData.get("stream-full").ave) }, 0);
            }
            if (statsData.containsKey("checkpoint-summary")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Checkpoint Summary", statsData.get("checkpoint-summary").diff }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Avg", statsData.get("input-rate").ave }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Min", statsData.get("input-rate").min }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Max", statsData.get("input-rate").max }, 0);
            }
            if (statsData.containsKey("output")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Output Diff", statsData.get("output").diff }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.SOURCE) {
            if (statsData.containsKey("input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Diff", statsData.get("input").diff }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Avg", statsData.get("rate").ave }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Min", statsData.get("rate").min }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Max", statsData.get("rate").max }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Avg", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").ave) }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Min", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").min) }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Max", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").max) }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.TARGET) {
            if (statsData.containsKey("input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Diff", statsData.get("input").diff }, 0);
            }
            if (statsData.containsKey("target-acked")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Target Acked Diff", statsData.get("target-acked").diff }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Avg", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").ave) }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Min", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").min) }, 0);
            }
            if (statsData.containsKey("cpu-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "CPU Max", MonitorModel.renderCpuPercent((long)statsData.get("cpu-rate").max) }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.STREAM) {
            if (statsData.containsKey("input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Diff", statsData.get("input").diff }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Avg", statsData.get("input-rate").ave }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Min", statsData.get("input-rate").min }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Max", statsData.get("input-rate").max }, 0);
            }
            if (statsData.containsKey("stream-full")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Backpressure %", asPercent(statsData.get("stream-full").ave) }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.WINDOW) {
            if (statsData.containsKey("input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Diff", statsData.get("input").diff }, 0);
            }
            if (statsData.containsKey("num-partitions")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Num Partitions Avg", statsData.get("num-partitions").ave }, 0);
            }
            if (statsData.containsKey("num-partitions")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Num Partitions Min", statsData.get("num-partitions").min }, 0);
            }
            if (statsData.containsKey("num-partitions")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Num Partitions Max", statsData.get("num-partitions").max }, 0);
            }
            if (statsData.containsKey("window-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Window Size Avg", statsData.get("window-size").ave }, 0);
            }
            if (statsData.containsKey("window-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Window Size Min", statsData.get("window-size").min }, 0);
            }
            if (statsData.containsKey("window-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Widnow Size Max", statsData.get("window-size").max }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.CACHE) {
            if (statsData.containsKey("cache-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Cache Size Avg", statsData.get("cache-size").ave }, 0);
            }
            if (statsData.containsKey("cache-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Cache Size Min", statsData.get("cache-size").min }, 0);
            }
            if (statsData.containsKey("cache-size")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Cache Size Max", statsData.get("cache-size").max }, 0);
            }
            if (statsData.containsKey("local-hits")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Local Hits Diff", statsData.get("local-hits").diff }, 0);
            }
            if (statsData.containsKey("local-misses")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Local Misses Diff", statsData.get("local-misses").diff }, 0);
            }
            if (statsData.containsKey("remote-hits")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Remote Hits Diff", statsData.get("remote-hits").diff }, 0);
            }
            if (statsData.containsKey("remote-misses")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Remote Misses Diff", statsData.get("remote-misses").diff }, 0);
            }
            if (statsData.containsKey("cache-refresh")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "First Cache Refresh", dateSecs(statsData.get("cache-refresh").min) }, 0);
            }
            if (statsData.containsKey("cache-refresh")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Last Cache Refresh", dateSecs(statsData.get("cache-refresh").max) }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Avg", statsData.get("rate").ave }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Min", statsData.get("rate").min }, 0);
            }
            if (statsData.containsKey("rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Rate Max", statsData.get("rate").max }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.CQ) {
            if (statsData.containsKey("input")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Diff", statsData.get("input").diff }, 0);
            }
            if (statsData.containsKey("output")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Output Diff", statsData.get("output").diff }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Avg", statsData.get("input-rate").ave }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Min", statsData.get("input-rate").min }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Max", statsData.get("input-rate").max }, 0);
            }
            if (statsData.containsKey("output-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Output Rate Avg", statsData.get("output-rate").ave }, 0);
            }
            if (statsData.containsKey("output-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Output Rate Min", statsData.get("output-rate").min }, 0);
            }
            if (statsData.containsKey("output-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Output Rate Max", statsData.get("output-rate").max }, 0);
            }
        }
        else if (componentInfo.getType() == EntityType.HDSTORE) {
            if (statsData.containsKey("hds-created")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "HDs Created Diff", statsData.get("hds-created").diff }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Avg", statsData.get("input-rate").ave }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Min", statsData.get("input-rate").min }, 0);
            }
            if (statsData.containsKey("input-rate")) {
                singleLine(target, "= %-50s%-82s=%n", new Object[] { "Input Rate Max", statsData.get("input-rate").max }, 0);
            }
        }
        else {
            for (final Map.Entry<String, Object> entry2 : statsData.entrySet()) {
                final String type = entry2.getKey();
                assert entry2.getValue() instanceof MonitorModel.MonitorStats;
                final MonitorModel.MonitorStats stats = (MonitorModel.MonitorStats)entry2.getValue();
                singleLine(target, "= %-50s%-82s=%n", humanizeData(type + " Avg", stats.ave), 0);
                singleLine(target, "= %-50s%-82s=%n", humanizeData(type + " Max", stats.max), 0);
                singleLine(target, "= %-50s%-82s=%n", humanizeData(type + " Min", stats.min), 0);
                singleLine(target, "= %-50s%-82s=%n", humanizeData(type + " Sum", stats.sum), 0);
            }
        }
        tableBottom(target);
    }
    
    private static Object asPercent(final Object value) {
        if (value instanceof Float) {
            return (float)value * 100.0f + "%";
        }
        try {
            return Float.valueOf(value.toString()) * 100.0f + "%";
        }
        catch (Exception e) {
            return value.toString();
        }
    }
    
    private static String dateSecs(final Object value) {
        if (value instanceof Long) {
            return CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
        }
        try {
            return CluiMonitorView.DATE_FORMAT_SECS.format(new Date(Long.valueOf(value.toString())));
        }
        catch (Exception e) {
            return value.toString();
        }
    }
    
    private static void timeSeriesReport(final scala.collection.mutable.StringBuilder target, final MetaInfo.MetaObject componentInfo, final MetaInfo.Server serverInfo, final Long startTime, final Long endTime, final String types) {
        final String[] fields = { "time-series" };
        final Map<String, Object> filter = new HashMap<String, Object>();
        if (serverInfo != null) {
            filter.put("serverInfo", serverInfo.uuid);
        }
        if (startTime != null) {
            filter.put("start-time", startTime);
        }
        if (endTime != null) {
            filter.put("end-time", endTime);
        }
        if (types != null && !types.isEmpty()) {
            filter.put("time-series-fields", types);
        }
        final Map<String, Object> values = getEntitySummary(componentInfo.uuid, fields, filter);
        if (values == null) {
            System.out.println("= " + componentInfo.name + " - data not ready yet");
            return;
        }
        final Map<String, Object> timeSeriesData = (Map<String, Object>)values.get("time-series-data");
        if (timeSeriesData == null || timeSeriesData.isEmpty()) {
            tableTitle(target, componentInfo.getFullName() + " Time Series Report");
            final Object[] columnNames = { "Property", "Value" };
            target.append(String.format("= %-50s%-82s=%n", columnNames));
            tableDivider(target);
            tableMessage(target, "No Time-Series Data Available For That Request");
            tableMessage(target, "");
            tableBottom(target);
        }
        else {
            scala.collection.mutable.StringBuilder[] lines = null;
            boolean first = true;
            for (final Map.Entry<String, Object> typeAndVals : timeSeriesData.entrySet()) {
                assert typeAndVals.getValue() instanceof List;
                final String type = typeAndVals.getKey();
                final List<Pair<Long, Object>> vals = (List<Pair<Long, Object>>)typeAndVals.getValue();
                if (first) {
                    lines = new scala.collection.mutable.StringBuilder[vals.size() + 1];
                    for (int i = 0; i < lines.length; ++i) {
                        lines[i] = new scala.collection.mutable.StringBuilder();
                    }
                    lines[0].append("Timestamp");
                    for (int i = 1; i < lines.length; ++i) {
                        final Pair<Long, Object> tsAndVal = vals.get(i - 1);
                        lines[i].append((Object)tsAndVal.first);
                    }
                    first = false;
                }
                for (final scala.collection.mutable.StringBuilder line : lines) {
                    line.append(",");
                }
                lines[0].append(type);
                assert vals.size() == lines.length - 1;
                for (int i = 1; i < lines.length; ++i) {
                    final Pair<Long, Object> tsAndVal = vals.get(i - 1);
                    lines[i].append(tsAndVal.second);
                }
            }
            for (final scala.collection.mutable.StringBuilder line2 : lines) {
                target.append(line2).append("\n");
            }
        }
    }
    
    private static Object[] humanizeData(String key, Object value) {
        if (key == null) {
            key = "?";
        }
        if (value == null) {
            value = "-";
        }
        if (key.equals("nodes")) {
            key = "Nodes";
        }
        else if (key.equals("cpu-thread")) {
            key = "CPU% (Thread)";
        }
        else if (key.equals("cpu-rate")) {
            key = "CPU Rate";
        }
        else if (key.equals("input")) {
            key = "Input";
        }
        else if (key.equals("rate")) {
            key = "Rate";
        }
        else if (key.equals("cpu")) {
            key = "CPU%";
        }
        else if (key.equals("input-rate")) {
            key = "Input Rate";
        }
        else if (key.equals("cpu-time")) {
            key = "CPU Time";
            if (value instanceof Long) {
                final Double doubleValue = (long)value / 1.0E9;
                value = doubleValue + " Seconds";
            }
        }
        else if (key.equals("timestamp")) {
            key = "Timestamp";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
            else if (value instanceof String) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date(Long.valueOf((String)value)));
            }
        }
        else if (key.equals("cache-refresh")) {
            key = "Cache Refresh";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("latest-activity")) {
            key = "Latest Activity";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("start")) {
            key = "Start";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("end")) {
            key = "End";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("stream-full")) {
            key = "Stream Full";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("first-event-time")) {
            key = "First Event (Time)";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("last-event-time")) {
            key = "Last Event (Time)";
            if (value instanceof Long) {
                value = CluiMonitorView.DATE_FORMAT_SECS.format(new Date((long)value));
            }
        }
        else if (key.equals("es-tx-bytes")) {
            key = "Elasticsearch Transmit Throughput";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else if (key.equals("es-rx-bytes")) {
            key = "Elasticsearch Receive Throughput";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else if (key.equals("es-total-bytes")) {
            key = "Elasticsearch Cluster Storage Total";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else if (key.equals("es-free-bytes")) {
            key = "Elasticsearch Cluster Storage Free";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else if (key.equals("kafka-bytes-rate")) {
            key = "Kafka Writes (Bytes per second)";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else if (key.equals("kafka-msgs-rate")) {
            key = "Kafka Writes (Messages per second)";
            if (value instanceof Long) {
                value = getMemoryString((long)value);
            }
        }
        else {
            final String[] ss = key.split("[-\\s]");
            final scala.collection.mutable.StringBuilder result = new scala.collection.mutable.StringBuilder();
            for (final String z : ss) {
                if (z.equals("cpu")) {
                    result.append("CPU");
                }
                else {
                    result.append(Character.toUpperCase(z.charAt(0))).append(z.substring(1));
                }
                result.append(" ");
            }
            key = result.toString();
        }
        return new Object[] { key, value };
    }
    
    private static Object getOneValue(final String name, final Map<String, Object> values, final Object defaultValue) {
        Object value = defaultValue;
        if (values.containsKey(name)) {
            value = values.get(name);
        }
        else if (values.containsKey("most-recent-data")) {
            final Map<String, Object> mostRecent = (Map<String, Object>)values.get("most-recent-data");
            if (mostRecent.containsKey(name)) {
                value = mostRecent.get(name);
            }
        }
        return value;
    }
    
    public static void singleLine(final scala.collection.mutable.StringBuilder target, final String lineFormat, final Object[] lineData, final int depth) {
        final scala.collection.mutable.StringBuilder pad = new scala.collection.mutable.StringBuilder();
        for (int i = 0; i < depth; ++i) {
            pad.append("  ");
        }
        lineData[0] = pad.toString() + lineData[0];
        target.append(String.format(lineFormat, lineData));
    }
    
    public static void printUsage() {
        System.out.println("usage: mon[itor] [component|application|node [node]] [-follow [\"<seconds>\"]] [-start \"<datetime>\"] [-end \"<datetime>\"]\n\nThe bare 'MONITOR' command prints a synopsis of the Nodes in the cluster and the Applications loaded. 'MON' is a synonym for 'MONITOR'.\n\nIf one parameter is given, it prints a detailed table of that thing whether it is an application, a component, or a node.\n\nA second parameter is for situations when a component is distributed to multiple nodes. The second parameter indicates the node and the data shown will only be for that application or component on that node.\n\nThe '-follow' flag will cause the data to be automatically reprinted periodically. It accepts an optional integer argument indicating the repeat period in seconds. The default is five seconds. Press RETURN to stop the loop.\n\nThe '-start' and '-end' flags require timestamp parameters inside quotation marks. Within any period, the most recent data is printed. The timstamps must be formatted in one of the following formats:\nyyyy/MM/dd-HH:mm:ss:SSS\nyyyy/MM/dd-HH:mm:ss\nyyyy/MM/dd-HH:mm (indicating a time of the current day)\nHH:mm\n\n");
    }
    
    private static String getMemoryString(final long memoryLong) {
        if (memoryLong < 0L) {
            return "?";
        }
        double memoryDouble = memoryLong;
        String memoryUnit = "b";
        if (memoryDouble > 1024.0) {
            memoryUnit = "Kb";
            memoryDouble /= 1024.0;
            if (memoryDouble > 1024.0) {
                memoryUnit = "Mb";
                memoryDouble /= 1024.0;
                if (memoryDouble > 1024.0) {
                    memoryUnit = "Gb";
                    memoryDouble /= 1024.0;
                }
            }
        }
        final String memoryString = String.format("%.2f", memoryDouble);
        final String result = memoryString + memoryUnit;
        return result;
    }
    
    private static String getUptimeString(final long uptime) {
        if (uptime < 0L) {
            return "?";
        }
        final long uptimeHours = uptime / 3600000L;
        long remainder = uptime % 3600000L;
        final long uptimeMinutes = remainder / 60000L;
        remainder %= 60000L;
        final long uptimeSeconds = remainder / 1000L;
        final String uptimeStr = String.format("%02d:%02d:%02d", uptimeHours, uptimeMinutes, uptimeSeconds);
        return uptimeStr;
    }
    
    private static void tableBottom(final scala.collection.mutable.StringBuilder target) {
        for (int i = 0; i < 135; ++i) {
            target.append("=");
        }
        target.append(System.lineSeparator());
    }
    
    static void tableDivider(final scala.collection.mutable.StringBuilder result) {
        result.append("= ");
        for (int i = 0; i < 131; ++i) {
            result.append("-");
        }
        result.append(" =").append(System.lineSeparator());
    }
    
    static void printTableDivider() {
        System.out.print("= ");
        for (int i = 0; i < 131; ++i) {
            System.out.print("-");
        }
        System.out.print(" =");
        System.out.println("");
    }
    
    public static void tableTitle(final scala.collection.mutable.StringBuilder target, final String title) {
        target.append("============ " + title + " ");
        for (int i = 0; i < 121 - title.length(); ++i) {
            target.append("=");
        }
        target.append(System.lineSeparator());
    }
    
    public static void printTableTitle(final String title) {
        System.out.print("============ " + title + " ");
        for (int i = 0; i < 121 - title.length(); ++i) {
            System.out.print("=");
        }
        System.out.println("");
    }
    
    private static void tableMessage(final scala.collection.mutable.StringBuilder target, final String message) {
        final int maxLineLength = 131;
        final scala.collection.mutable.StringBuilder result = new scala.collection.mutable.StringBuilder();
        int startOffset = 0;
        do {
            int endOffset;
            int spacePadding;
            if (message.length() - startOffset < maxLineLength) {
                endOffset = message.length();
                spacePadding = maxLineLength - (endOffset - startOffset);
            }
            else {
                endOffset = startOffset + maxLineLength;
                spacePadding = 0;
                for (int spaceOffset = 0; spaceOffset < 10; ++spaceOffset) {
                    if (message.charAt(endOffset - spaceOffset) == ' ') {
                        spacePadding = spaceOffset;
                        endOffset -= spacePadding;
                        break;
                    }
                }
            }
            result.append("= ");
            result.append(message.substring(startOffset, endOffset));
            for (int i = 0; i < spacePadding; ++i) {
                result.append(" ");
            }
            result.append(" =\n");
            startOffset = endOffset;
        } while (startOffset < message.length());
        target.append(result);
    }
    
    private static Map<String, Object> getEntitySummary(final UUID objectUuid, final String[] fields, final Map<String, Object> filter) {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
        if (srvs.isEmpty()) {
            throw new RuntimeException("No available Node to get Monitor data");
        }
        final HDKey wk = new HDKey(objectUuid, null);
        final RemoteCall action = new MonitoringApiQueryHandler("WAStatistics", wk, fields, filter, null, false);
        try {
            final Collection<Map<HDKey, Map<String, Object>>> manyResults = DistributedExecutionManager.exec(hz, (Callable<Map<HDKey, Map<String, Object>>>)action, srvs);
            if (manyResults.size() != 1) {
                CluiMonitorView.logger.warn((Object)("Expected result from exactly one node, but received " + manyResults.size()));
                return null;
            }
            final Map<HDKey, Map<String, Object>> x = manyResults.iterator().next();
            if (x.isEmpty()) {
                return null;
            }
            if (x.size() != 1) {
                CluiMonitorView.logger.warn((Object)("Expected exactly 1 HD result, but found " + x.size()));
                return null;
            }
            return x.values().iterator().next();
        }
        catch (Exception e) {
            CluiMonitorView.logger.error((Object)e);
            return null;
        }
    }
    
    private static Long convertToTimestamp(final String timestampString) {
        try {
            if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d")) {
                return CluiMonitorView.DATE_FORMAT_MILLIS.parse(timestampString).getTime();
            }
            if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d")) {
                return CluiMonitorView.DATE_FORMAT_SECS.parse(timestampString).getTime();
            }
            if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d")) {
                return CluiMonitorView.DATE_FORMAT_MINS.parse(timestampString).getTime();
            }
            if (timestampString.matches("\\d\\d:\\d\\d")) {
                final Calendar c = Calendar.getInstance();
                final int year = c.get(1);
                final int month = c.get(2) + 1;
                final int day = c.get(5);
                final String fullTimestampString = String.format("%04d/%02d/%02d-%s", year, month, day, timestampString);
                return CluiMonitorView.DATE_FORMAT_MINS.parse(fullTimestampString).getTime();
            }
            if (timestampString.matches("\\d\\d:\\d\\d")) {
                return Long.valueOf(timestampString);
            }
        }
        catch (ParseException e) {
            CluiMonitorView.logger.error((Object)"Parse exception getting ", (Throwable)e);
        }
        return null;
    }
    
    private static void executeCommand(final MetaInfo.MetaObject entityInfo, final MetaInfo.Server serverInfo, final boolean verbose, final long followTime, final Long startTime, final Long endTime, final String format, final String types, final String export) throws MetaDataRepositoryException {
        try {
            boolean keepFollowing = followTime >= 0L;
            final BufferedReader IO = new BufferedReader(new InputStreamReader(System.in));
            if (keepFollowing) {
                System.out.println("Press any key to stop following");
            }
            do {
                final long loopStartTime = System.currentTimeMillis();
                final scala.collection.mutable.StringBuilder target = new scala.collection.mutable.StringBuilder();
                if (format != null && format.equalsIgnoreCase("TimeSeriesReport")) {
                    timeSeriesReport(target, entityInfo, serverInfo, startTime, endTime, types);
                }
                else if (format != null && format.equalsIgnoreCase("SummaryReport")) {
                    summaryReport(target, entityInfo, serverInfo, startTime, endTime, types);
                }
                else if (entityInfo == null) {
                    basicMonitorStatus(target, verbose, startTime, endTime);
                }
                else if (entityInfo instanceof MetaInfo.Flow && serverInfo == null) {
                    final MetaInfo.Flow fo = (MetaInfo.Flow)entityInfo;
                    applicationBlock(target, fo, serverInfo, true, startTime, endTime);
                }
                else if (entityInfo instanceof MetaInfo.Server && serverInfo == null) {
                    final MetaInfo.Server so = (MetaInfo.Server)entityInfo;
                    serverBlock(target, so, startTime, endTime);
                }
                else {
                    componentBlock(target, entityInfo, serverInfo, startTime, endTime, types, export != null);
                }
                export(target, export);
                while (keepFollowing && System.currentTimeMillis() - loopStartTime < followTime) {
                    if (IO.ready()) {
                        IO.read();
                        keepFollowing = false;
                        break;
                    }
                    Thread.sleep(20L);
                }
            } while (keepFollowing);
        }
        catch (IOException e) {
            CluiMonitorView.logger.error((Object)"Error while following entity", (Throwable)e);
        }
        catch (InterruptedException e2) {
            CluiMonitorView.logger.error((Object)"Follow-thread interrupted", (Throwable)e2);
        }
    }
    
    public static void export(final scala.collection.mutable.StringBuilder text, final String to) {
        if (to == null || to.isEmpty()) {
            System.out.println(text);
        }
        else {
            final List<String> lines = Arrays.asList(text.toString());
            final Path file = Paths.get(to, new String[0]);
            try {
                Files.write(file, lines, Charset.forName("UTF-8"), new OpenOption[0]);
            }
            catch (IOException e) {
                final String err = "Could not write data to file " + to + ": " + e.getMessage();
                System.out.println(err);
                CluiMonitorView.logger.error((Object)err);
            }
        }
    }
    
    public static JsonNode showLag(final UUID appID, final List<String> param_list, final AuthToken sessionID) throws MetaDataRepositoryException {
        final MetaInfo.Flow metaObject = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(appID, HSecurityManager.TOKEN);
        try {
            if (param_list != null && !param_list.isEmpty() && param_list.get(0).equalsIgnoreCase("PATHS")) {
                final Set<MetaInfo.MetaObject> startingPoints = metaObject.getStartingPoints();
                final Set filteredStartingPoint = startingPoints.stream().filter(p -> p.type == EntityType.SOURCE).collect(Collectors.toSet());
                final Set<MetaInfo.MetaObject> endPoints = metaObject.getEndPoints();
                final Set filteredEndPoints = endPoints.stream().filter(p -> p.type == EntityType.HDSTORE || p.type == EntityType.TARGET).collect(Collectors.toSet());
                metaObject.findAllPaths(filteredStartingPoint, filteredEndPoints);
                return null;
            }
        }
        catch (Exception e) {
            if (CluiMonitorView.logger.isInfoEnabled()) {
                CluiMonitorView.logger.info((Object)e.getMessage(), (Throwable)e);
            }
        }
        final String[] fields = new String[0];
        final Map<String, Object> filter = new HashMap<String, Object>();
        final Map<String, Object> values = getEntitySummary(metaObject.uuid, fields, filter);
        if (values == null) {
            System.out.println("= " + metaObject.name + " - data not ready yet");
            return null;
        }
        final Object lag = getOneValue("lag-report", values, ".");
        final Object rate = getOneValue("lag-rate", values, ".");
        System.out.println("\nAverage Latency Rate is: " + rate + " milli seconds");
        if (lag == "." || rate == ".") {
            MetaInfo.StatusInfo.Status stat = null;
            try {
                final AppManagerRequestClient appManagerRequestHandler = new AppManagerRequestClient(sessionID);
                final ChangeApplicationStateResponse response = appManagerRequestHandler.sendRequest(ActionType.STATUS, metaObject, null);
                appManagerRequestHandler.close();
                stat = ((ApplicationStatusResponse)response).getStatus();
            }
            catch (Exception e2) {
                if (CluiMonitorView.logger.isInfoEnabled()) {
                    CluiMonitorView.logger.info((Object)e2.getMessage(), (Throwable)e2);
                }
            }
            if (stat != null && stat == MetaInfo.StatusInfo.Status.RUNNING) {
                System.out.println("=  Latency marker did not go through the whole application " + metaObject.name);
            }
            else {
                System.out.println("=  Need to start the application " + metaObject.name + " to see latency results");
            }
            return null;
        }
        System.out.println("\nAverage Latency Rate is: " + rate);
        if (param_list != null && !param_list.isEmpty()) {
            System.out.println("Detailed latency for the application " + metaObject.getFullName() + ": \n");
            final String[] response2 = ((String)lag).split(",");
            final String[] sortedResponse = sortBySourceTime(response2);
            for (int i = 0; i < sortedResponse.length; ++i) {
                System.out.println(sortedResponse[i].split(":", 2)[1] + "\n");
            }
        }
        return null;
    }
    
    public static String[] sortBySourceTime(final String[] response) {
        final List<String> resultList = new ArrayList<String>();
        final TreeMap<Date, List<String>> map = new TreeMap<Date, List<String>>();
        final String regex = " at time : (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})";
        for (int i = 0; i < response.length; ++i) {
            try {
                final Matcher m = Pattern.compile(regex).matcher(response[i]);
                if (m.find()) {
                    final Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(m.group(1));
                    if (map.get(date) == null) {
                        final ArrayList<String> responses = new ArrayList<String>();
                        responses.add(response[i]);
                        map.put(date, responses);
                    }
                    else {
                        map.get(date).add(response[i]);
                    }
                }
            }
            catch (ParseException e) {
                CluiMonitorView.logger.error((Object)("Error while parsing latency information : " + e.getMessage()));
            }
        }
        for (final Map.Entry<Date, List<String>> entry : map.entrySet()) {
            final List<String> value = entry.getValue();
            resultList.addAll(value);
        }
        return resultList.toArray(new String[0]);
    }
    
    static {
        CluiMonitorView.logger = Logger.getLogger((Class)CluiMonitorView.class);
        DATE_FORMAT_MILLIS = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS");
        DATE_FORMAT_SECS = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        DATE_FORMAT_MINS = new SimpleDateFormat("yyyy/MM/dd-HH:mm");
        (DEFINED_REPORTS = new HashSet<String>()).add("TimeSeriesReport");
        CluiMonitorView.DEFINED_REPORTS.add("SummaryReport");
        CluiMonitorView.detailView = false;
    }
}
