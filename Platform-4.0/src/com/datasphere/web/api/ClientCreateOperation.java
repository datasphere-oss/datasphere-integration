package com.datasphere.web.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Lists;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.DeploymentStrategy;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.stmts.DeploymentRule;
import com.datasphere.runtime.compiler.stmts.ExceptionHandler;
import com.datasphere.runtime.compiler.stmts.MappedStream;
import com.datasphere.runtime.compiler.stmts.OutputClause;
import com.datasphere.runtime.compiler.stmts.RecoveryDescription;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;

public class ClientCreateOperation
{
    private ObjectMapper jsonMapper;
    private MDRepository mdRepository;
    Object resultAsJavaObject;
    
    public ClientCreateOperation() {
        this.jsonMapper = ObjectMapperFactory.getInstance();
        this.mdRepository = MetadataRepository.getINSTANCE();
        this.resultAsJavaObject = null;
    }
    
    public Object createOrUpdate(final QueryValidator clientInterface, final EntityType entityType, final Boolean cor, final ObjectNode data, final String namespace, final AuthToken requestToken, final AuthToken token) throws Exception {
        final String objectName = data.get("name").asText(" ");
        switch (entityType) {
            case APPLICATION: {
                final List<Property> eHandlersList = Lists.newArrayList();
                final JsonNode eHandlers = data.get("eHandlers");
                if (eHandlers != null) {
                    for (final ExceptionType exceptionType : ExceptionType.values()) {
                        final String exceptionName = exceptionType.name();
                        final JsonNode handlerValue = eHandlers.get(exceptionName);
                        if (handlerValue != null) {
                            eHandlersList.add(new Property(exceptionName, handlerValue.asText()));
                        }
                    }
                }
                final ExceptionHandler exceptionHandler = new ExceptionHandler(eHandlersList);
                Boolean encrypt = false;
                if (data.get("encrypted") != null) {
                    encrypt = data.get("encrypted").asBoolean();
                }
                RecoveryDescription rd = null;
                final JsonNode recoveryType = data.get("recoveryType");
                final JsonNode recoveryPeriod = data.get("recoveryPeriod");
                if (recoveryType != null && recoveryPeriod != null) {
                    rd = new RecoveryDescription(recoveryType.asInt(), recoveryPeriod.asLong());
                }
                final JsonNode importStatements = data.get("importStatements");
                String[] importStatementsArray = null;
                if (importStatements != null) {
                    assert importStatements.isArray();
                    importStatementsArray = arrayNodeToStringArray(importStatements, entityType, "importStatements");
                }
                final JsonNode deploymentPlan = data.get("deploymentInfo");
                final List<DeploymentRule> deploymentRules = Lists.newArrayList();
                if (deploymentPlan != null) {
                    assert deploymentPlan.isArray();
                    for (final JsonNode deploymentPlanDetail : deploymentPlan) {
                        final DeploymentStrategy strategy = DeploymentStrategy.valueOf(deploymentPlanDetail.get("strategy").asText());
                        final String flow = deploymentPlanDetail.get("flowName").asText();
                        final String group = deploymentPlanDetail.get("dg").asText();
                        deploymentRules.add(new DeploymentRule(strategy, flow, group));
                    }
                }
                clientInterface.CreateAppStatement(requestToken, objectName, cor, null, encrypt, rd, exceptionHandler, importStatementsArray, deploymentRules);
                break;
            }
            case STREAM: {
                final JsonNode jsonSNode = data.get("partitioningFields");
                assert jsonSNode.isArray();
                final String[] streamPartitionFields = arrayNodeToStringArray(data.get("partitioningFields"), entityType, "partitioningFields");
                final String dataType = data.get("dataType").asText(" ");
                final String type_fullName = getFullName(dataType);
                final boolean persist = data.get("persist").asBoolean(false);
                StreamPersistencePolicy spp;
                if (persist) {
                    String propertySet = data.get("propertySet").asText("default");
                    if (!propertySet.equalsIgnoreCase("default")) {
                        propertySet = getFullName(propertySet);
                    }
                    spp = new StreamPersistencePolicy(propertySet);
                }
                else {
                    spp = new StreamPersistencePolicy(null);
                }
                clientInterface.CreateStreamStatement(requestToken, objectName, cor, streamPartitionFields, type_fullName, spp);
                break;
            }
            case WINDOW: {
                final JsonNode jsonWNode = data.get("partitioningFields");
                assert jsonWNode.isArray();
                final String streamName = data.get("stream").asText(" ");
                boolean isJumping = false;
                final String mode = data.get("mode").asText("sliding");
                if (mode.equalsIgnoreCase("jumping") || mode.equalsIgnoreCase("session")) {
                    isJumping = true;
                }
                final boolean isPersistent = Boolean.FALSE;
                final String[] pFields_2 = arrayNodeToStringArray(jsonWNode, entityType, "partitioningFields");
                final ObjectNode window_len = (ObjectNode)data.get("size");
                final Map<String, Object> window_len_properties = this.extractIntervalPolicyProperties(window_len);
                Map<String, Object> slidePolicyProperties;
                if (!isJumping) {
                    slidePolicyProperties = this.extractSlidePolicyProperties(window_len);
                }
                else {
                    slidePolicyProperties = Collections.emptyMap();
                }
                final String stream_fullName = getFullName(streamName);
                clientInterface.CreateWindowStatement(requestToken, objectName, cor, stream_fullName, window_len_properties, isJumping, pFields_2, isPersistent, slidePolicyProperties);
                break;
            }
            case TYPE: {
                final ArrayNode textFields = (ArrayNode)data.get("fields");
                final TypeField[] allFields = new TypeField[textFields.size()];
                for (int ii = 0; ii < allFields.length; ++ii) {
                    final ObjectNode field = (ObjectNode)textFields.get(ii);
                    final TypeField tF = new TypeField(field.get("name").asText(" "), new TypeName(field.get("type").asText(" "), 0), (field.get("isKey") == null) ? Boolean.FALSE : field.get("isKey").asBoolean((boolean)Boolean.FALSE));
                    allFields[ii] = tF;
                }
                clientInterface.CreateTypeStatement_New(requestToken, objectName, cor, allFields);
                break;
            }
            case CQ: {
                final String destStream = data.get("output").asText(" ");
                final String destStream_fullName = getFullName(destStream);
                final List<String> fieldList = new ArrayList<String>();
                final String selectText = data.get("select").asText(" ");
                String uiConfig = null;
                if (data.has("uiconfig")) {
                    uiConfig = data.get("uiconfig").toString();
                }
                clientInterface.CreateCqStatement(requestToken, objectName, cor, destStream_fullName, fieldList.toArray(new String[0]), selectText, uiConfig);
                break;
            }
            case SOURCE: {
                final String adapterHandler_S = data.get("adapter").get("handler").asText(" ");
                final String adapterHandler_S_fullName = getFullNameFromVersionedHandlerString(adapterHandler_S);
                final String adapterVersion = getVersionFromVersionedHandlerString(adapterHandler_S);
                final Map<String, String> adapterProps_S = (Map<String, String>)this.convertJsonNodeToMap(data.get("adapter").get("properties"));
                final List<Property> adapterPropList_S = new ArrayList<Property>();
                for (final Map.Entry<String, String> entry : adapterProps_S.entrySet()) {
                    adapterPropList_S.add(new Property(entry.getKey(), entry.getValue()));
                }
                String parserHandler_S_fullName = null;
                String parserVersion = null;
                final List<Property> parserPropList = new ArrayList<Property>();
                final JsonNode parserNode = data.get("parser");
                if (parserNode != null) {
                    String parserHandler_S;
                    if (parserNode.get("handler") != null) {
                        parserHandler_S = parserNode.get("handler").asText(" ");
                    }
                    else {
                        parserHandler_S = " ";
                    }
                    if (StringUtils.isNotBlank((CharSequence)parserHandler_S)) {
                        parserHandler_S_fullName = getFullNameFromVersionedHandlerString(parserHandler_S);
                        parserVersion = getVersionFromVersionedHandlerString(parserHandler_S);
                    }
                    Map<String, String> parserProps;
                    if (parserNode.get("properties") != null) {
                        parserProps = (Map<String, String>)this.convertJsonNodeToMap(parserNode.get("properties"));
                    }
                    else {
                        parserProps = Collections.emptyMap();
                    }
                    for (final Map.Entry<String, String> entry2 : parserProps.entrySet()) {
                        parserPropList.add(new Property(entry2.getKey(), entry2.getValue()));
                    }
                }
                final ArrayNode outputClauseArray = (ArrayNode)data.get("outputclause");
                if (outputClauseArray != null) {
                    final List<OutputClause> outputClauses = new ArrayList<OutputClause>(outputClauseArray.size());
                    for (int outputClauseCounter = 0; outputClauseCounter < outputClauseArray.size(); ++outputClauseCounter) {
                        final JsonNode outputClauseJSON = outputClauseArray.get(outputClauseCounter);
                        String selectQuery = null;
                        if (outputClauseJSON.get("select") != null) {
                            selectQuery = outputClauseJSON.get("select").asText();
                        }
                        String outputStream_fullName = null;
                        if (outputClauseJSON.get("outputStream") != null) {
                            final String outputStream = outputClauseJSON.get("outputStream").asText();
                            outputStream_fullName = getFullName(outputStream);
                        }
                        final JsonNode mapJSON = outputClauseJSON.get("map");
                        MappedStream mappedStream = null;
                        if (mapJSON != null) {
                            mappedStream = (MappedStream)this.jsonMapper.convertValue((Object)mapJSON, (Class)MappedStream.class);
                            mappedStream.streamName = getFullName(mappedStream.streamName);
                        }
                        List<TypeField> fields = null;
                        if (outputClauseJSON.get("fields") != null && !(outputClauseJSON.get("fields") instanceof NullNode)) {
                            final ArrayNode typeDefJSON = (ArrayNode)outputClauseJSON.get("fields");
                            if (typeDefJSON != null) {
                                fields = new ArrayList<TypeField>(typeDefJSON.size());
                                for (int ii2 = 0; ii2 < typeDefJSON.size(); ++ii2) {
                                    final ObjectNode field2 = (ObjectNode)typeDefJSON.get(ii2);
                                    final TypeField tF2 = new TypeField(field2.get("name").asText(" "), new TypeName(field2.get("type").asText(" "), 0), (field2.get("isKey") == null) ? Boolean.FALSE : field2.get("isKey").asBoolean((boolean)Boolean.FALSE));
                                    fields.add(tF2);
                                }
                            }
                        }
                        final OutputClause outputClause = new OutputClause(outputStream_fullName, mappedStream, null, fields, null, selectQuery);
                        outputClauses.add(outputClause);
                    }
                    clientInterface.CreateFilteredSourceStatement_New(requestToken, objectName, cor, adapterHandler_S_fullName, adapterVersion, adapterPropList_S, parserHandler_S_fullName, parserVersion, parserPropList, outputClauses);
                    break;
                }
                final String outputStream2 = data.get("outputStream").asText(" ");
                final String outputStream_fullName2 = getFullName(outputStream2);
                clientInterface.CreateSourceStatement_New(requestToken, objectName, cor, adapterHandler_S_fullName, adapterVersion, adapterPropList_S, parserHandler_S_fullName, parserVersion, parserPropList, outputStream_fullName2);
                break;
            }
            case TARGET: {
                final String adapterHandler_T = data.get("adapter").get("handler").asText(" ");
                final String adapterHandler_T_fullName = getFullNameFromVersionedHandlerString(adapterHandler_T);
                final String adapterVersion_T = getVersionFromVersionedHandlerString(adapterHandler_T);
                final Map<String, Object> adapterProps_T = (Map<String, Object>)this.convertJsonNodeToMap(data.get("adapter").get("properties"));
                Map<String, String>[] adapterPropList_T = null;
                if (adapterProps_T != null) {
                    adapterPropList_T = (Map<String, String>[])new Map[adapterProps_T.size()];
                    int cc = 0;
                    for (final Map.Entry<String, Object> entry3 : adapterProps_T.entrySet()) {
                        final Map<String, String> aTempMap = new HashMap<String, String>();
                        final Object value = entry3.getValue();
                        final Object correspondingEncrypted = adapterProps_T.get(entry3.getKey() + "_encrypted");
                        if (correspondingEncrypted != null && value instanceof Map) {
                            aTempMap.put(entry3.getKey(), (value == null) ? null : ((Map)value).get("encrypted").toString());
                        }
                        else {
                            aTempMap.put(entry3.getKey(), (value == null) ? null : value.toString());
                        }
                        adapterPropList_T[cc++] = aTempMap;
                    }
                }
                String formatterHandler_fullName = null;
                String formatterVersion = null;
                Map<String, String>[] formatterPropList = (Map<String, String>[])new Map[0];
                final JsonNode formatter = data.get("formatter");
                if (formatter != null) {
                    final String formatterHandler = data.get("formatter").get("handler").asText(" ");
                    formatterHandler_fullName = getFullNameFromVersionedHandlerString(formatterHandler);
                    formatterVersion = getVersionFromVersionedHandlerString(formatterHandler);
                    final Map<String, Object> formatterProps = (Map<String, Object>)this.convertJsonNodeToMap(data.get("formatter").get("properties"));
                    formatterPropList = (Map<String, String>[])new Map[formatterProps.size()];
                    int dd = 0;
                    for (final Map.Entry<String, Object> entry4 : formatterProps.entrySet()) {
                        final Map<String, String> fTempMap = new HashMap<String, String>();
                        fTempMap.put(entry4.getKey(), (entry4.getValue() == null) ? null : entry4.getValue().toString());
                        formatterPropList[dd++] = fTempMap;
                    }
                }
                final String inputStream = data.get("inputStream").asText(" ");
                final String inputStream_fullName = getFullName(inputStream);
                clientInterface.CreateTargetStatement(requestToken, objectName, cor, adapterHandler_T_fullName, adapterVersion_T, adapterPropList_T, formatterHandler_fullName, formatterVersion, formatterPropList, inputStream_fullName);
                break;
            }
            case FLOW: {
                clientInterface.CreateFlowStatement(requestToken, objectName, cor, null);
                break;
            }
            case PROPERTYSET: {
                final ArrayNode propertyArrayNode = (ArrayNode)data.get("properties");
                final Map<String, String>[] propertyList = (Map<String, String>[])new Map[propertyArrayNode.size()];
                for (int ii3 = 0; ii3 < propertyList.length; ++ii3) {
                    final ObjectNode field3 = (ObjectNode)propertyArrayNode.get(ii3);
                    final String fieldName = field3.get("name").asText(" ");
                    final String fieldValue = field3.get("value").asText(" ");
                    final Map mm = new HashMap();
                    mm.put(fieldName, fieldValue);
                    propertyList[ii3] = (Map<String, String>)mm;
                }
                clientInterface.CreatePropertySet(token, objectName, cor, propertyList);
                break;
            }
            case HDSTORE: {
                final String contextType = data.get("contextType").asText(" ");
                final String contextType_fullName = getFullName(contextType);
                final Map<String, List<String>> eventTypes = new HashMap<String, List<String>>();
                final ArrayNode eventArray = (ArrayNode)data.get("eventTypes");
                for (int jj = 0; jj < eventArray.size(); ++jj) {
                    final ObjectNode eventType = (ObjectNode)eventArray.get(jj);
                    if (eventType != null) {
                        final String eventTypeName = getFullName(eventType.get("typename").asText());
                        final TextNode uuid = (TextNode)eventType.get("id");
                        final TextNode fqn = (TextNode)eventType.get("typename");
                        final TextNode keyFields = (TextNode)eventType.get("keyField");
                        final List<String> keyNodeList = Arrays.asList(keyFields.asText());
                        eventTypes.put(eventTypeName, keyNodeList);
                    }
                }
                final JsonNode persistence_node = data.get("persistence");
                String howLong = null;
                Map<String, String>[] providerPropsList = null;
                if (persistence_node != null) {
                    final JsonNode frequency_node = persistence_node.get("frequency");
                    if (frequency_node != null) {
                        howLong = frequency_node.asText(" ");
                    }
                    final JsonNode persistence_properties_node = persistence_node.get("properties");
                    if (persistence_properties_node != null) {
                        final Map<String, String> providerProps = (Map<String, String>)this.convertJsonNodeToMap(persistence_properties_node);
                        providerPropsList = (Map<String, String>[])new Map[providerProps.size()];
                        int zz = 0;
                        for (final Map.Entry<String, String> entry5 : providerProps.entrySet()) {
                            final Map<String, String> map = new HashMap<String, String>();
                            map.put(entry5.getKey(), entry5.getValue());
                            providerPropsList[zz++] = map;
                        }
                    }
                }
                clientInterface.CreateWASWithTypeNameStatement(requestToken, objectName, cor, contextType_fullName, eventTypes, howLong, providerPropsList);
            }
            case CACHE: {
                final String adapterHandler_C = data.get("adapter").get("handler").asText(" ");
                final String adapterHandler_C_fullName = getFullName(adapterHandler_C);
                final Map<String, String> adapterProps_C = (Map<String, String>)this.convertJsonNodeToMap(data.get("adapter").get("properties"));
                final List<Property> adapterPropList_C = new ArrayList<Property>();
                for (final Map.Entry<String, String> entry5 : adapterProps_C.entrySet()) {
                    adapterPropList_C.add(new Property(entry5.getKey(), entry5.getValue()));
                }
                String parserHandler_C_fullName = null;
                final List<Property> parserPropList_C = new ArrayList<Property>();
                final JsonNode parser = data.get("parser");
                if (parser != null) {
                    final String parserHandler_C = parser.get("handler").asText(" ");
                    parserHandler_C_fullName = getFullName(parserHandler_C);
                    final Map<String, String> parserProps_C = (Map<String, String>)this.convertJsonNodeToMap(data.get("parser").get("properties"));
                    for (final Map.Entry<String, String> entry6 : parserProps_C.entrySet()) {
                        parserPropList_C.add(new Property(entry6.getKey(), entry6.getValue()));
                    }
                }
                final Map<String, String> cacheProps_C = (Map<String, String>)this.convertJsonNodeToMap(data.get("queryProperties"));
                final List<Property> cachePropList_C = new ArrayList<Property>();
                for (final Map.Entry<String, String> entry6 : cacheProps_C.entrySet()) {
                    cachePropList_C.add(new Property(entry6.getKey(), entry6.getValue()));
                }
                final String typeName = data.get("typename").asText(" ");
                final String typeName_fullName = getFullName(typeName);
                clientInterface.CreateCacheStatement_New(requestToken, objectName, cor, adapterHandler_C_fullName, adapterPropList_C, parserHandler_C_fullName, parserPropList_C, cachePropList_C, typeName_fullName);
            }
            case WI: {}
            case ALERTSUBSCRIBER: {}
            case SERVER: {}
            case USER: {}
            case ROLE: {}
            case INITIALIZER: {}
            case DG: {}
            case NAMESPACE: {
                clientInterface.CreateNameSpaceStatement(requestToken, objectName, cor);
            }
            case STREAM_GENERATOR: {}
            case SORTER: {}
            case WASTOREVIEW: {}
            case DASHBOARD: {
                clientInterface.CreateDashboard(token, objectName, namespace, cor, data);
                break;
            }
            case PAGE: {
                clientInterface.CreatePage(token, objectName, namespace, cor, data);
                break;
            }
            case QUERYVISUALIZATION: {
                clientInterface.CreateQueryVisualization(token, objectName, namespace, cor, data);
            }
        }
        return this.resultAsJavaObject = this.mdRepository.getMetaObjectByName(entityType, namespace, objectName, null, token);
    }
    
    public Map convertJsonNodeToMap(final JsonNode jsonNode) {
        return (Map)this.jsonMapper.convertValue((Object)jsonNode, (Class)Map.class);
    }
    
    public static String getFullName(final String fqn) {
        final String[] parts = fqn.split("\\.");
        if (parts.length > 3) {
            final StringBuilder invalidName = new StringBuilder();
            for (int iter = 2; iter < parts.length; ++iter) {
                invalidName.append(parts[iter]);
                if (iter != parts.length - 1) {
                    invalidName.append(".");
                }
            }
            throw new RuntimeException(String.format("Invalid name given: %s", invalidName.toString()));
        }
        return parts[0].concat(".").concat(parts[2]);
    }
    
    public static String[] getFullName(final String[] fieldList) {
        for (int ii = 0; ii < fieldList.length; ++ii) {
            fieldList[ii] = getFullName(fieldList[ii]);
        }
        return fieldList;
    }
    
    public static String getFullNameFromVersionedHandlerString(final String versionedFN) {
        final String[] fn_version = versionedFN.split("_");
        return getFullName(fn_version[0]);
    }
    
    public static String getVersionFromVersionedHandlerString(final String versionedFN) {
        final String[] fn_version = versionedFN.split("_");
        if (fn_version.length == 2) {
            return fn_version[1];
        }
        return " ";
    }
    
    public static String[] arrayNodeToStringArray(final JsonNode jsonNode, final EntityType entityType, final String partitionfields) throws NullPointerException {
        if (jsonNode == null) {
            throw new NullPointerException("Unexpected Null Value for field: " + partitionfields + " while creating entity of type: " + entityType.name());
        }
        final List<String> paramList = new ArrayList<String>();
        final ArrayNode nn = (ArrayNode)jsonNode;
        final Iterator<JsonNode> itr = (Iterator<JsonNode>)nn.elements();
        while (itr.hasNext()) {
            final String param = itr.next().asText(" ");
            paramList.add(param);
        }
        return paramList.toArray(new String[paramList.size()]);
    }
    
    public static Set<String> arrayNodeToSet(final JsonNode jsonNode) throws NullPointerException {
        if (jsonNode == null) {
            throw new NullPointerException("Unexpected Null Value while converting json node to a Set");
        }
        final Set<String> paramList = new HashSet<String>();
        final ArrayNode nn = (ArrayNode)jsonNode;
        final Iterator<JsonNode> itr = (Iterator<JsonNode>)nn.elements();
        while (itr.hasNext()) {
            final String param = itr.next().asText(" ");
            paramList.add(param);
        }
        return paramList;
    }
    
    public static void main(final String[] args) throws Exception {
        final Server server = new Server();
        server.initializeHazelcast();
        final HSecurityManager securityManager = HSecurityManager.get();
        final AuthToken TOKEN = securityManager.authenticate("admin", "asd");
        final ClientCreateOperation cco = new ClientCreateOperation();
        final QueryValidator qv = new QueryValidator(server);
        qv.createNewContext(TOKEN);
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode on = (ObjectNode)mapper.readTree("{   \"name\":\"source1\", \"adapter\": {\"handler\":\"admin.source.CSVReader\",  \"properties\":{\"directory\":\"/home/\"}}, \"outputclause\":[{\"map\":{\"streamName\":\"admin.stream.test2\",\"mappingProperties\":{\"table\":\"SCOTTTIGER\"}},\"select\":\"select TO_INT(data[0]) where TO_INT(data[0]) > 10\"},{\"outputStream\":\"admin.stream.test\",\"fields\":[{\"name\":\"a\", \"type\":\"int\", \"isKey\":\"false\"}],\"select\":\"select TO_INT(data[0]) where TO_INT(data[0]) > 10\"},{\"outputStream\":\"admin.stream.test4\",\"fields\":null,\"select\":\"select $all where TO_INT(data[0]) > 10\"},{\"outputStream\":\"admin.stream.test3\"},{\"map\":{\"streamName\":\"admin.stream.test5\",\"mappingProperties\":{\"table\":\"SCOTTTIGER\"}}}]}, }");
        cco.createOrUpdate(qv, EntityType.SOURCE, Boolean.TRUE, on, "admin", TOKEN, null);
    }
    
    public static void main2(final String[] args) {
        final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
        final ObjectNode rootNode = jsonMapper.createObjectNode();
        final ArrayNode dataSourceArray = jsonMapper.createArrayNode();
        dataSourceArray.add("foo");
        dataSourceArray.add("bar");
        rootNode.set("inputs", (JsonNode)dataSourceArray);
        try {
            final String[] strings = (String[])jsonMapper.readValue(rootNode.get("inputs").asText(" "), (Class)String[].class);
            System.out.println(strings);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private Map<String, Object> extractIntervalPolicyProperties(final ObjectNode intervalPolicy) {
        final Map<String, Object> intervalPolicyProperties = new HashMap<String, Object>();
        if (intervalPolicy != null) {
            final JsonNode count_node = intervalPolicy.get("count");
            if (count_node != null) {
                intervalPolicyProperties.put("count", count_node.intValue());
            }
            final JsonNode time_node = intervalPolicy.get("time");
            if (time_node != null) {
                intervalPolicyProperties.put("time", time_node.asText());
            }
            final JsonNode attr_node = intervalPolicy.get("onField");
            if (attr_node != null) {
                intervalPolicyProperties.put("on", attr_node.asText());
            }
            final JsonNode range_node = intervalPolicy.get("timeout");
            if (range_node != null) {
                intervalPolicyProperties.put("range", range_node.longValue());
            }
        }
        return intervalPolicyProperties;
    }
    
    private Map<String, Object> extractSlidePolicyProperties(final ObjectNode intervalPolicy) {
        final Map<String, Object> slidePolicyProperties = new HashMap<String, Object>();
        if (intervalPolicy != null) {
            final JsonNode outputInterval = intervalPolicy.get("outputInterval");
            if (outputInterval != null) {
                final JsonNode count_node = intervalPolicy.get("count");
                if (count_node != null) {
                    slidePolicyProperties.put("count", outputInterval.longValue());
                }
                final JsonNode time_node = intervalPolicy.get("time");
                if (time_node != null) {
                    slidePolicyProperties.put("time", outputInterval.longValue());
                }
            }
        }
        return slidePolicyProperties;
    }
    
    public Object alter(final QueryValidator clientInterface, final EntityType entityType, final ObjectNode data, final String namespace, final AuthToken requestToken, final AuthToken token) throws Exception {
        final String objectName = data.get("name").asText(" ");
        switch (entityType) {
            case STREAM: {
                final JsonNode jsonSNode = data.get("partitioningFields");
                assert jsonSNode.isArray();
                Boolean enablePartition = null;
                String[] streamPartitionFields = null;
                if (data.has("partitioningFields")) {
                    streamPartitionFields = arrayNodeToStringArray(data.get("partitioningFields"), entityType, "partitioningFields");
                    if (streamPartitionFields == null) {
                        enablePartition = null;
                    }
                    else if (streamPartitionFields.length == 0) {
                        enablePartition = null;
                    }
                    else {
                        enablePartition = true;
                    }
                }
                Boolean enablePersistence = null;
                StreamPersistencePolicy spp = null;
                if (data.has("persist")) {
                    enablePersistence = data.get("persist").asBoolean(false);
                    if (enablePersistence) {
                        String propertySet = null;
                        if (data.has("propertySet")) {
                            propertySet = data.get("propertySet").asText("default");
                        }
                        if (propertySet == null) {
                            propertySet = "default";
                        }
                        if (!propertySet.equalsIgnoreCase("default")) {
                            propertySet = getFullName(propertySet);
                        }
                        spp = new StreamPersistencePolicy(propertySet);
                    }
                }
                clientInterface.AlterStreamStatement(requestToken, objectName, enablePartition, streamPartitionFields, enablePersistence, spp);
                break;
            }
        }
        return this.resultAsJavaObject = this.mdRepository.getMetaObjectByName(entityType, namespace, objectName, null, token);
    }
}
