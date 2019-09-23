package com.datasphere.rest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;

import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.InvalidUriException;
import com.datasphere.exception.SecurityException;
import com.datasphere.health.HealthMonitor;
import com.datasphere.health.HealthMonitorImpl;
import com.datasphere.health.HealthRecordCollection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.HD;

public class HealthServlet extends MainServlet
{
    private static Logger logger;
    private HealthMonitor healthMonitor;
    ObjectMapper objectMapper;
    HSecurityManager securityManager;
    
    public HealthServlet() {
        this.objectMapper = ObjectMapperFactory.newInstance();
        this.securityManager = HSecurityManager.get();
        this.healthMonitor = new HealthMonitorImpl();
    }
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final int responseStatus = 200;
        String responseData = "";
        String objectType = "healthRecords";
        HealthRecordCollection healthRecordCollection = null;
        Object partialHealthData = null;
        boolean requestedPartialHealthData = false;
        final String path = request.getPathInfo();
        final Map<String, String[]> parameterMap = new HashMap<String, String[]>(request.getParameterMap());
        try {
            final AuthToken token = this.getToken(request);
            final MetaInfo.User user = this.securityManager.getAuthenticatedUser(token);
            if (user == null) {
                throw new SecurityException("No valid User found for token: " + token);
            }
            HealthServlet.logger.debug((Object)("Request for health data received from user: " + user.getFullName()));
            parameterMap.remove("token");
            if (path == null || path.equals("/")) {
                healthRecordCollection = this.filterHealthRecordsByCount(null, null);
            }
            else if (path.startsWith("/")) {
                final String[] subParts = path.split("/");
                if (subParts.length == 2) {
                    if (subParts[1].equalsIgnoreCase("healthRecords")) {
                        healthRecordCollection = this.getHealthRecordCollection(parameterMap);
                    }
                    else if (subParts[1].equalsIgnoreCase("issues")) {
                        objectType = "issues";
                        healthRecordCollection = this.getHealthRecordBySubObject("issuesList", parameterMap);
                    }
                    else if (subParts[1].equalsIgnoreCase("statusChanges")) {
                        objectType = "statusChanges";
                        healthRecordCollection = this.getHealthRecordBySubObject("stateChangeList", parameterMap);
                    }
                    else {
                        final UUID uuid = new UUID(subParts[1]);
                        healthRecordCollection = this.healthMonitor.getHealtRecordById(uuid);
                    }
                }
                else if (subParts.length > 2) {
                    final UUID uuid = new UUID(subParts[1]);
                    healthRecordCollection = this.healthMonitor.getHealtRecordById(uuid);
                    partialHealthData = this.extractRequestedField(healthRecordCollection, subParts, uuid);
                    requestedPartialHealthData = true;
                }
            }
            if (requestedPartialHealthData) {
                responseData = this.objectMapper.writeValueAsString(partialHealthData);
                this.writeResult(responseData, response, responseStatus);
            }
            else if ("healthRecords".equalsIgnoreCase(objectType) && healthRecordCollection != null) {
                responseData = this.objectMapper.writeValueAsString((Object)healthRecordCollection);
                this.writeResult(responseData, response, responseStatus);
            }
            else if ("issues".equalsIgnoreCase(objectType) && healthRecordCollection != null) {
                responseData = this.getResponseForIssues(healthRecordCollection);
                this.writeResult(responseData, response, responseStatus);
            }
            else if ("statusChanges".equalsIgnoreCase(objectType) && healthRecordCollection != null) {
                responseData = this.getResponseForStatusChanges(healthRecordCollection);
                this.writeResult(responseData, response, responseStatus);
            }
            else {
                this.writeResult(responseData, response, responseStatus);
            }
        }
        catch (SearchPhaseExecutionException spee) {
            this.writeException((Exception)spee, response, 204);
        }
        catch (JsonProcessingException jpe) {
            this.writeException((Exception)jpe, response, 400);
        }
        catch (SecurityException se) {
            this.writeException(se, response, 401);
        }
        catch (InvalidUriException iue) {
            this.writeException(iue, response, 400);
        }
        catch (Exception e) {
            this.writeException(e, response, 400);
        }
    }
    
    private HealthRecordCollection getHealthRecordCollection(final Map<String, String[]> parameterMap) {
        HealthRecordCollection healthRecordCollection = null;
        try {
            if (parameterMap.size() > 0) {
                healthRecordCollection = this.processRequestByParameters(parameterMap);
            }
            else {
                this.filterHealthRecordsByCount(null, null);
            }
        }
        catch (Exception ex) {}
        return healthRecordCollection;
    }
    
    private HealthRecordCollection getHealthRecordBySubObject(final String field, final Map<String, String[]> parameterMap) throws Exception {
        final String[] startParam = parameterMap.get(String.valueOf(Suburi.start));
        final String[] endParam = parameterMap.get(String.valueOf(Suburi.end));
        if (startParam == null || endParam == null) {
            throw new InvalidUriException("start and end parameters are required.");
        }
        final long startTime = Long.parseLong(startParam[0]);
        final long endTime = Long.parseLong(endParam[0]);
        if ("issuesList".equalsIgnoreCase(field)) {
            return this.healthMonitor.getHealthRecordsWithIssuesList(startTime, endTime);
        }
        if ("stateChangeList".equalsIgnoreCase(field)) {
            return this.healthMonitor.getHealthRecordsWithStatusChangeList(startTime, endTime);
        }
        throw new InvalidUriException("Invalid URI");
    }
    
    private String getResponseForIssues(final HealthRecordCollection healthRecordCollection) throws JsonProcessingException {
        final List<ObjectNode> issuesArray = new ArrayList<ObjectNode>();
        for (final HD hd : healthRecordCollection.healthRecords) {
            final JsonNode issuesListNode = hd.get("issuesList");
            if (issuesListNode.isArray()) {
                final ArrayNode issuesList = (ArrayNode)issuesListNode;
                for (final JsonNode issue : issuesList) {
                    final ObjectNode issueNode = ObjectMapperFactory.getInstance().createObjectNode();
                    issueNode.put("issue", issue.get("issue").toString());
                    issueNode.put("fqName", issue.get("fqName").toString());
                    issueNode.put("componentType", issue.get("componentType").toString());
                    issueNode.put("time", hd.get("startTime").longValue());
                    issuesArray.add(issueNode);
                }
            }
        }
        return ObjectMapperFactory.newInstance().writeValueAsString((Object)issuesArray);
    }
    
    private String getResponseForStatusChanges(final HealthRecordCollection healthRecordCollection) throws JsonProcessingException {
        final List<ObjectNode> statusChangesArray = new ArrayList<ObjectNode>();
        for (final HD hd : healthRecordCollection.healthRecords) {
            final JsonNode statusChangesNode = hd.get("stateChangeList");
            if (statusChangesNode.isArray()) {
                final ArrayNode statusChangesList = (ArrayNode)statusChangesNode;
                for (final JsonNode statusChange : statusChangesList) {
                    final ObjectNode statusChangeNode = ObjectMapperFactory.getInstance().createObjectNode();
                    statusChangeNode.put("timestamp", statusChange.get("timestamp").longValue());
                    statusChangeNode.put("previousStatus", statusChange.get("previousStatus").toString());
                    statusChangeNode.put("currentStatus", statusChange.get("currentStatus").toString());
                    statusChangeNode.put("fqName", statusChange.get("fqName").toString());
                    statusChangeNode.put("type", statusChange.get("type").toString());
                    statusChangesArray.add(statusChangeNode);
                }
            }
        }
        return ObjectMapperFactory.newInstance().writeValueAsString((Object)statusChangesArray);
    }
    
    private HealthRecordCollection processRequestByParameters(final Map<String, String[]> parameterMap) throws Exception {
        HealthRecordCollection healthRecordCollection = null;
        if (parameterMap.get(String.valueOf(Suburi.size)) != null) {
            healthRecordCollection = this.filterHealthRecordsByCount(parameterMap.get(String.valueOf(Suburi.size)), parameterMap.get(String.valueOf(Suburi.from)));
        }
        else {
            if (parameterMap.get(String.valueOf(Suburi.start)) == null) {
                throw new Exception("Invalid URI for request");
            }
            healthRecordCollection = this.filterHealthRecordsByTime(parameterMap.get(String.valueOf(Suburi.start)), parameterMap.get(String.valueOf(Suburi.end)));
        }
        return healthRecordCollection;
    }
    
    private HealthRecordCollection filterHealthRecordsByTime(final String[] strings0, final String[] strings1) throws Exception {
        final long startTime = Long.parseLong(strings0[0]);
        long endTime = -1L;
        if (strings1 != null && strings1[0] != null) {
            endTime = Long.parseLong(strings1[0]);
        }
        final HealthRecordCollection healthRecordCollection = this.healthMonitor.getHealthRecordsByTime(startTime, endTime);
        return healthRecordCollection;
    }
    
    private HealthRecordCollection filterHealthRecordsByCount(final String[] strings0, final String[] strings1) throws Exception {
        final int size = (strings0 != null) ? ((strings0[0] != null) ? Integer.parseInt(strings0[0]) : 1) : 1;
        final long from = (strings1 != null) ? ((strings1[0] != null) ? Long.parseLong(strings1[0]) : 0L) : 0L;
        final HealthRecordCollection healthRecordCollection = this.healthMonitor.getHealthRecordsByCount(size, from);
        if (healthRecordCollection != null) {
            healthRecordCollection.next = "/healthRecords?size=" + size + "&from=" + (from + size);
            if (from - size < 0L) {
                healthRecordCollection.prev = "/healthRecords?size=" + size + "&from=0";
            }
            else {
                healthRecordCollection.prev = "/healthRecords?size=" + size + "&from=" + (from - size);
            }
        }
        return healthRecordCollection;
    }
    
    private Object extractRequestedField(final HealthRecordCollection healthRecordCollection, final String[] subParts, final UUID uuid) throws Exception {
        HD healthRecordHD = null;
        if (healthRecordCollection.healthRecords.iterator().hasNext()) {
            healthRecordHD = healthRecordCollection.healthRecords.iterator().next();
        }
        if (healthRecordHD == null) {
            throw new NullPointerException("Did not find a health record with id: " + uuid);
        }
        final Suburi suburi = Suburi.valueOf(subParts[2]);
        final String actualFieldName = suburi.getHrFieldName();
        if (actualFieldName == null) {
            throw new NullPointerException("No matching field found URI sub path : " + suburi);
        }
        final Object result = healthRecordHD.get(actualFieldName);
        return result;
    }
    
    private void writeException(final Exception e, final HttpServletResponse response, final int responseStatus) throws IOException {
        response.setStatus(responseStatus);
        response.resetBuffer();
        final String errorMessage = (e.getMessage() != null) ? e.getMessage() : "Exception raised but no error message, please see server log for stack trace.";
        HealthServlet.logger.error((Object)errorMessage, (Throwable)e);
        if (e instanceof SearchPhaseExecutionException) {
            response.getWriter().write("No health data yet!");
        }
        else {
            response.getWriter().write(errorMessage);
        }
    }
    
    private void writeResult(final String responseData, final HttpServletResponse response, final int responseStatus) throws IOException {
        response.setStatus(responseStatus);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        final byte[] bytes = responseData.getBytes(Charset.defaultCharset());
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
    }
    
    static {
        HealthServlet.logger = Logger.getLogger((Class)HealthServlet.class);
    }
    
    private enum Suburi
    {
        start("start"), 
        end("end"), 
        size("size"), 
        from("from"), 
        clustersize("clusterSize"), 
        servers("serverHealthMap"), 
        apps("appHealthMap"), 
        derby("derbyAlive"), 
        es("elasticSearch"), 
        agents("agentCount"), 
        sources("sourceHealthMap"), 
        wastores("waStoreHealthMap"), 
        targets("targetHealthMap"), 
        caches("cacheHealthMap"), 
        statechanges("stateChangeList"), 
        issues("issuesList");
        
        private final String hrFieldName;
        
        private Suburi(final String hrFieldName) {
            this.hrFieldName = hrFieldName;
        }
        
        public String getHrFieldName() {
            return this.hrFieldName;
        }
    }
}
