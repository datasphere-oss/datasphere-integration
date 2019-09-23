package com.datasphere.hdstore.base;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.elasticsearch.cluster.metadata.AliasOrIndex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.google.common.collect.Sets;
import com.datasphere.hdstore.CheckpointManager;
import com.datasphere.hdstore.Utility;
import com.datasphere.hdstore.HDStore;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.checkpoints.Manager;
import com.datasphere.hdstore.constants.Capability;
import com.datasphere.hdstore.constants.NameType;
import com.datasphere.hdstore.elasticsearch.IndexOperationsManager;
import com.datasphere.hdstore.exceptions.CapabilityException;
import com.datasphere.hdstore.exceptions.QuerySchemaException;
import com.datasphere.hdstore.exceptions.HDStoreException;
import com.datasphere.hdstore.exceptions.HDStoreMissingException;

public abstract class HDStoreManagerBase implements HDStoreManager
{
    private static final Class<HDStoreManagerBase> thisClass;
    private static final Logger logger;
    private static final String CLASS_NAME;
    private static final JsonSchema hdQuerySchema;
    private static final Level invalidQueryLoggingLevel;
    private static final boolean isInvalidQueryLoggingEnabled;
    private final EnumSet<Capability> capabilities;
    private final String providerName;
    private final String instanceName;
    private final Manager checkpointManager;
    private IndexOperationsManager indexManager;
    
    protected HDStoreManagerBase(final String providerName, final String instanceName, final Capability... capabilities) {
        this.capabilities = EnumSet.noneOf(Capability.class);
        this.indexManager = null;
        this.providerName = providerName;
        this.instanceName = instanceName;
        this.checkpointManager = new Manager(this);
        this.indexManager = new IndexOperationsManager(this);
        Collections.addAll(this.capabilities, capabilities);
    }
    
    public IndexOperationsManager getIndexManager() {
        return this.indexManager;
    }
    
    @Override
    public String getInstanceName() {
        return this.instanceName;
    }
    
    @Override
    public String getProviderName() {
        return this.providerName;
    }
    
    @Override
    public Set<Capability> getCapabilities() {
        return Collections.unmodifiableSet((Set<? extends Capability>)this.capabilities);
    }
    
    @Override
    public CheckpointManager getCheckpointManager() {
        return this.checkpointManager;
    }
    
    @Override
    public HDStore getOrCreate(final String hdStoreName, final Map<String, Object> properties) {
        HDStore result = this.getUsingAlias(hdStoreName, properties);
        if (result == null) {
            result = this.create(hdStoreName, properties);
            if (result == null) {
                result = this.get(hdStoreName, properties);
                if (result == null) {
                    throw new HDStoreException(String.format("Unable to create HDStore '%s'", hdStoreName));
                }
            }
        }
        return result;
    }
    
    protected void validateQuery(final JsonNode queryJson) throws HDStoreException {
        if (HDStoreManagerBase.hdQuerySchema == null) {
            throw new IllegalArgumentException("hdQuerySchema");
        }
        if (queryJson == null) {
            throw new IllegalArgumentException("queryJson");
        }
        final ProcessingReport report = HDStoreManagerBase.hdQuerySchema.validateUnchecked(queryJson);
        if (report != null && !report.isSuccess()) {
            processInvalidQuery(queryJson, (Iterable<ProcessingMessage>)report);
            return;
        }
        verifyPresent(queryJson, "select");
        verifyMissing(queryJson, "delete");
        this.validateCapability(queryJson, "select", Capability.SELECT);
        this.validateCapability(queryJson, "from", Capability.FROM);
        this.validateCapability(queryJson, "where", Capability.WHERE);
        this.validateCapability(queryJson, "orderby", Capability.ORDER_BY);
        this.validateCapability(queryJson, "groupby", Capability.GROUP_BY);
        this.validateCapability(queryJson, "having", Capability.HAVING);
        this.validateSelectClause(queryJson);
        this.validateFromClause(queryJson);
    }
    
    protected void validateDelete(final JsonNode queryJson) throws HDStoreException {
        if (HDStoreManagerBase.hdQuerySchema == null) {
            throw new IllegalArgumentException("hdQuerySchema");
        }
        if (queryJson == null) {
            throw new IllegalArgumentException("queryJson");
        }
        final ProcessingReport report = HDStoreManagerBase.hdQuerySchema.validateUnchecked(queryJson);
        if (report != null && !report.isSuccess()) {
            processInvalidQuery(queryJson, (Iterable<ProcessingMessage>)report);
            return;
        }
        verifyPresent(queryJson, "delete");
        verifyMissing(queryJson, "select");
        this.validateCapability(queryJson, "delete", Capability.DELETE);
        this.validateCapability(queryJson, "from", Capability.FROM);
        this.validateCapability(queryJson, "where", Capability.WHERE);
        verifyMissing(queryJson, "orderby");
        verifyMissing(queryJson, "groupby");
        verifyMissing(queryJson, "having");
        validateDeleteClause(queryJson);
        this.validateFromClause(queryJson);
    }
    
    private static void processInvalidQuery(final JsonNode queryJson, final Iterable<ProcessingMessage> report) throws QuerySchemaException {
        if (HDStoreManagerBase.isInvalidQueryLoggingEnabled) {
            HDStoreManagerBase.logger.log((Priority)HDStoreManagerBase.invalidQueryLoggingLevel, (Object)String.format("Query : %s", queryJson));
            int messageNumber = 1;
            for (final ProcessingMessage message : report) {
                final JsonNode instanceNode = message.asJson().path("instance").path("pointer");
                final String instanceNodeName = instanceNode.isMissingNode() ? "<Unknown>" : instanceNode.asText().replace("/", "");
                final String messageText = message.getMessage();
                HDStoreManagerBase.logger.log((Priority)HDStoreManagerBase.invalidQueryLoggingLevel, (Object)String.format("Error %2d: %s : %s", messageNumber, instanceNodeName, messageText));
                ++messageNumber;
            }
        }
        throw new QuerySchemaException(queryJson, report);
    }
    
    private void validateCapability(final JsonNode queryJson, final String feature, final Capability capability) throws CapabilityException {
        if (!queryJson.path(feature).isMissingNode() && !this.capabilities.contains(capability)) {
            throw new CapabilityException(this.getProviderName(), capability.name());
        }
    }
    
    private static void verifyPresent(final JsonNode queryJson, final String feature) throws CapabilityException {
        if (queryJson.path(feature).isMissingNode()) {
            throw new QuerySchemaException(queryJson, String.format("JSON does not contain '%s' node, which is required", feature));
        }
    }
    
    private static void verifyMissing(final JsonNode queryJson, final String feature) throws CapabilityException {
        if (!queryJson.path(feature).isMissingNode()) {
            throw new QuerySchemaException(queryJson, String.format("JSON contains '%s' node, which is not allowed", feature));
        }
    }
    
    private void validateSelectClause(final JsonNode queryJson) throws CapabilityException {
        if (!this.capabilities.contains(Capability.PROJECTIONS)) {
            final ArrayNode selectNode = (ArrayNode)queryJson.path("select");
            if (selectNode.size() != 1 || !"*".equals(selectNode.get(0).asText())) {
                throw new CapabilityException(this.getProviderName(), Capability.PROJECTIONS.name());
            }
        }
    }
    
    private static void validateDeleteClause(final JsonNode queryJson) throws CapabilityException {
        final ArrayNode deleteNode = (ArrayNode)queryJson.path("delete");
        if (deleteNode.size() != 1 || !"*".equals(deleteNode.get(0).asText())) {
            throw new QuerySchemaException(queryJson, String.format("JSON contains '%s' node, which is not allowed", "*"));
        }
    }
    
    private void validateFromClause(final JsonNode queryJson) throws HDStoreException {
        this.validateMultipleFrom(queryJson);
        this.validateHDStoreNames(queryJson);
    }
    
    private void validateMultipleFrom(final JsonNode queryJson) throws CapabilityException {
        if (!this.capabilities.contains(Capability.FROM_MULTIPLE)) {
            final ArrayNode fromNode = (ArrayNode)queryJson.path("from");
            if (fromNode.size() != 1) {
                throw new CapabilityException(this.getProviderName(), Capability.FROM_MULTIPLE.name());
            }
        }
    }
    
    private void validateHDStoreNames(final JsonNode queryJson) throws HDStoreMissingException {
        final Set<String> hdStoreNames = Sets.newHashSet(this.getNames());
        final SortedMap<String, AliasOrIndex> map_alias = this.indexManager.getAliasMapFromES();
        for (final JsonNode fromName : queryJson.path("from")) {
            final String hdStoreName = fromName.asText();
            final String translatedName = this.translateName(NameType.HDSTORE, hdStoreName);
            if (!hdStoreNames.contains(translatedName) && !map_alias.containsKey("search_alias_" + translatedName)) {
                throw new HDStoreMissingException(this.getProviderName(), this.getInstanceName(), hdStoreName);
            }
        }
    }
    
    @Override
    public String toString() {
        return HDStoreManagerBase.CLASS_NAME + '{' + "provider=" + this.providerName + "instance=" + this.instanceName + "capabilities=" + this.capabilities + '}';
    }
    
    static {
        thisClass = HDStoreManagerBase.class;
        logger = Logger.getLogger((Class)HDStoreManagerBase.thisClass);
        CLASS_NAME = HDStoreManagerBase.thisClass.getSimpleName();
        hdQuerySchema = Utility.createJsonSchema("/HDStoreQuery.json");
        invalidQueryLoggingLevel = Level.DEBUG;
        isInvalidQueryLoggingEnabled = HDStoreManagerBase.logger.isEnabledFor((Priority)HDStoreManagerBase.invalidQueryLoggingLevel);
    }
}
