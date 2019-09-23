package com.datasphere.hdstore.elasticsearch;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.apache.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.datasphere.anno.EntryPoint;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.BaseServer;
import com.datasphere.hdstore.CheckpointManager;
import com.datasphere.hdstore.Utility;
import com.datasphere.hdstore.HD;
import com.datasphere.hdstore.HDStores;
import com.datasphere.hdstore.base.HDStoreManagerBase;
import com.datasphere.hdstore.constants.Capability;
import com.datasphere.hdstore.constants.Constants;
import com.datasphere.hdstore.constants.NameType;
import com.datasphere.hdstore.exceptions.CapabilityException;
import com.datasphere.hdstore.exceptions.ConnectionException;
import com.datasphere.hdstore.exceptions.HDStoreException;
import com.datasphere.hdstore.exceptions.HDStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

public class HDStoreManager extends HDStoreManagerBase
{
    private static final Class<HDStoreManager> thisClass;
    private static final Logger logger;
    private static final Pattern SERVER_NAME_SEPARATOR;
    private static final Pattern SERVER_PORT_SEPARATOR;
    private static final Pattern REMOTE_SERVER_CHARACTERS;
    private static final String REMOTE_SERVER_SUBSTITUTE_PATTERN = "_";
    private static final Locale HDSTORE_LOCALE;
    private static final Capability[] Capabilities;
    private static final long CONNECTION_RETRY_INTERVAL = 2000L;
    private static final String PROVIDER_NAME = "elasticsearch";
    private final String clusterName;
    private final String dataPath;
    private final String homePath;
    private final Integer maxLocalStorageNodes;
    private Node node;
    private Client client;
    
    public HDStoreManager(final String providerName, final Map<String, Object> properties, final String instanceName) {
        super(providerName, instanceName, HDStoreManager.Capabilities);
        this.node = null;
        this.client = null;
        final String externalEsClusterName = System.getProperty("com.datasphere.config.external-es");
        if (externalEsClusterName != null) {
            this.clusterName = externalEsClusterName;
        }
        else {
            this.clusterName = getClusterName(properties);
        }
        this.dataPath = Utility.getPropertyString(properties, "elasticsearch.data_path");
        this.homePath = Utility.getPropertyString(properties, "elasticsearch.home_path");
        this.maxLocalStorageNodes = Utility.getPropertyInteger(properties, "elasticsearch.max_local_storage_nodes");
    }
    
    private Client connectTransportClient(final String serverIdentity) {
        final String[] servers = HDStoreManager.SERVER_NAME_SEPARATOR.split(serverIdentity);
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("client.transport.sniff", true);
        if (this.dataPath != null) {
            settingsBuilder.put("path.data", this.dataPath);
        }
        String clusterName = null;
        TransportClient transportClient = null;
        for (final String server : servers) {
            final String[] tokens = HDStoreManager.SERVER_PORT_SEPARATOR.split(server);
            final String serverName = tokens[0];
            final String serverClusterName = (tokens.length > 1) ? tokens[1] : clusterName;
            if (clusterName == null && serverClusterName != null) {
                clusterName = serverClusterName;
                settingsBuilder.put("cluster.name", clusterName);
                transportClient = (TransportClient)new PreBuiltTransportClient(settingsBuilder.build(), new Class[0]);
            }
            if (serverClusterName == null) {
                HDStoreManager.logger.error(String.format("Do not have a cluster name for server '%s'", serverName));
            }
            if (serverClusterName != null && !serverClusterName.equals(clusterName)) {
                HDStoreManager.logger.error(String.format("Cluster name '%s' for server '%s' does not match primary cluster name '%s'", serverClusterName, serverName, clusterName));
                transportClient = null;
            }
            if (transportClient != null) {
                transportClient.addTransportAddresses(new TransportAddress[] { new InetSocketTransportAddress(new InetSocketAddress(serverName, 9300)) });
            }
        }
        final boolean isValid = transportClient != null && !transportClient.connectedNodes().isEmpty();
        reportTransportConnectResult(serverIdentity, clusterName, isValid);
        return (Client)transportClient;
    }
    
    private static void reportTransportConnectResult(final String serverIdentity, final String clusterName, final boolean isValid) {
        if (isValid) {
            HDStoreManager.logger.info(String.format("Connected to cluster '%s'", clusterName));
        }
        else {
            HDStoreManager.logger.error(String.format("Cannot connect to '%s'", serverIdentity));
        }
    }
    
    private static String getDefaultClusterName() {
        String result = null;
        final String clusterName = HazelcastSingleton.getClusterName();
        if (clusterName != null) {
            final Matcher matcher = HDStoreManager.REMOTE_SERVER_CHARACTERS.matcher(clusterName);
            result = matcher.replaceAll("_");
        }
        return result;
    }
    
    @NotNull
    private static String getClusterName(final Map<String, Object> properties) {
        final Object clusterNameObject = (properties == null) ? null : properties.get("elasticsearch.cluster_name");
        String clusterName = (clusterNameObject == null) ? null : clusterNameObject.toString();
        if (clusterName == null || clusterName.isEmpty()) {
            clusterName = getDefaultClusterName();
        }
        if (clusterName == null || clusterName.isEmpty()) {
            clusterName = "<Local>";
        }
        return clusterName;
    }
    
    @EntryPoint(usedBy = 2)
    public static String getInstanceName(final Map<String, Object> properties) {
        final String clusterName = getClusterName(properties);
        return "elasticsearch$" + clusterName;
    }
    
    private static String getIndexStoreType(final Object storeType, final String hdStoreName) {
        String result;
        if (storeType == null || "disk".equals(storeType) || "any".equals(storeType)) {
            result = "fs";
        }
        else {
            if (!"memory".equals(storeType)) {
                throw new HDStoreException(String.format("Cannot create HDStore '%s'. Property 'elasticsearch.storage_type' must be one of 'any', 'disk', or 'memory'", hdStoreName));
            }
            result = "memory";
        }
        return result;
    }
    
    private static boolean isValidHDStoreName(final String hdStoreName) {
        return hdStoreName != null && !hdStoreName.isEmpty();
    }
    
    private Client connectNodeClient(final String nodeName, final String clusterName) {
        final String networkHost = HazelcastSingleton.getBindingInterface();
        final String unicastHosts = this.getUnicastHosts();
        final String httpEnabled = System.getProperty("com.datasphere.config.es.http.enabled", "true");
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("network.host", networkHost).put("node.name", nodeName).put("discovery.zen.ping.unicast.hosts", unicastHosts).put("path.home", (this.homePath != null) ? this.homePath : "./elasticsearch").put("http.enabled", httpEnabled).put("http.type", "netty4").put("node.max_local_storage_nodes", (this.maxLocalStorageNodes != null) ? ((int)this.maxLocalStorageNodes) : Integer.MAX_VALUE);
        StatusLogger.getLogger().setLevel(Level.OFF);
        if (this.dataPath != null) {
            settingsBuilder.put("path.data", this.dataPath);
        }
        final String whitelistHosts = System.getProperty("reindex.remote.whitelist", null);
        if (whitelistHosts != null) {
            settingsBuilder.put("reindex.remote.whitelist", whitelistHosts);
        }
        HDStoreManager.logger.info(String.format("Cluster '%s' has nodes '%s'", clusterName, unicastHosts));
        final Collection plugins = Arrays.asList(Netty4Plugin.class, ReindexPlugin.class);
        final Settings settings = settingsBuilder.put("cluster.name", clusterName).build();
        try {
            this.node = new PluginConfigurableNode(settings, plugins).start();
        }
        catch (NodeValidationException e) {
            HDStoreManager.logger.error(e);
        }
        Client result = null;
        final Client nodeClient = this.node.client();
        final long startTime = System.currentTimeMillis();
        int attempts = 1;
        while (attempts <= 5) {
            try {
                final ClusterHealthRequest healthRequest = new ClusterHealthRequest();
                final int numDataNodes = ((ClusterHealthResponse)nodeClient.admin().cluster().health(healthRequest).actionGet()).getNumberOfDataNodes();
                HDStoreManager.logger.info(("ElasticSearch number of data nodes in the cluster: " + numDataNodes));
                result = nodeClient;
            }
            catch (ClusterBlockException exception) {
                if (exception.retryable()) {
                    try {
                        Thread.sleep(2000L);
                    }
                    catch (InterruptedException ex) {}
                    ++attempts;
                    continue;
                }
                HDStoreManager.logger.error(String.format("Cannot connect to '%s'", clusterName), (Throwable)exception);
            }
            break;
        }
        if (HDStoreManager.logger.isInfoEnabled()) {
            final long endTime = System.currentTimeMillis() - startTime;
            HDStoreManager.logger.info(("Connecting elasticsearch node client took " + endTime + " ms"));
        }
        final boolean isValid = result != null;
        reportTransportConnectResult(clusterName, clusterName, isValid);
        return nodeClient;
    }
    
    private String getUnicastHosts() {
        final String localHost = HazelcastSingleton.getBindingInterface();
        final Collection<String> unicastHosts = new ArrayList<String>(1);
        final Collection<Member> members = getClusterMembers();
        unicastHosts.add(localHost);
        for (final Member member : members) {
            addUnicastHost(unicastHosts, member);
        }
        return Joiner.on(",").join((Iterable)unicastHosts);
    }
    
    private static void addUnicastHost(final Collection<String> unicastHosts, final Member member) {
        final SocketAddress socketAddress = member.getSocketAddress();
        if (socketAddress instanceof InetSocketAddress) {
            final InetSocketAddress inetSocketAddress = (InetSocketAddress)socketAddress;
            final String addressString = inetSocketAddress.getAddress().getHostAddress();
            if (!unicastHosts.contains(addressString)) {
                unicastHosts.add(addressString);
            }
        }
    }
    
    private static Collection<Member> getClusterMembers() {
        final HazelcastInstance instance = HazelcastSingleton.get();
        final Cluster cluster = instance.getCluster();
        final Set<Member> members = (Set<Member>)cluster.getMembers();
        return (Collection<Member>)((members != null) ? members : new ArrayList<Member>(0));
    }
    
    private Client connectLocalClient() {
        try {
            this.node = new Node(Settings.builder().put("transport.type", "local").build()).start();
        }
        catch (NodeValidationException e) {
            HDStoreManager.logger.error(e);
        }
        HDStoreManager.logger.info(String.format("Connected to cluster '%s'", "<Local>"));
        return this.node.client();
    }
    
    @Override
    public String translateName(final NameType nameType, final String name) {
        if (nameType == NameType.HDSTORE) {
            return name.toLowerCase(HDStoreManager.HDSTORE_LOCALE);
        }
        return name;
    }
    
    @Override
    public String[] getNames() {
        String[] result = null;
        if (this.isConnected()) {
            result = ((ClusterStateResponse)this.client.admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().getConcreteAllIndices();
        }
        return result;
    }
    
    @Override
    public HDStore get(String hdStoreName, final Map<String, Object> properties) {
        HDStore result = null;
        if (isValidHDStoreName(hdStoreName) && this.isConnected()) {
            final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
            final IndicesExistsRequest request = Requests.indicesExistsRequest(new String[] { actualName });
            try {
                final IndicesExistsResponse response = (IndicesExistsResponse)this.client.admin().indices().exists(request).actionGet();
                if (response.isExists()) {
                    if (properties != null && properties.containsKey("elasticsearch.time_to_live")) {
                        properties.put("hd_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
                        properties.remove("elasticsearch.time_to_live");
                    }
                    hdStoreName = this.getActualNameWithoutTimeStamp(hdStoreName);
                    result = new HDStore(this, hdStoreName, properties, actualName);
                }
            }
            catch (NoNodeAvailableException exception) {
                throw new ConnectionException(this.getProviderName(), this.getInstanceName(), (Exception)exception);
            }
        }
        return result;
    }
    
    private boolean checkIfIndexExists(final String hdStoreName) {
        final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
        final IndicesExistsRequest request = Requests.indicesExistsRequest(new String[] { actualName });
        try {
            final IndicesExistsResponse response = (IndicesExistsResponse)this.client.admin().indices().exists(request).actionGet();
            if (response.isExists()) {
                return true;
            }
        }
        catch (NoNodeAvailableException exception) {
            throw new ConnectionException(this.getProviderName(), this.getInstanceName(), (Exception)exception);
        }
        return false;
    }
    
    public String getActualNameWithoutTimeStamp(final String actualName) {
        if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
            final String[] splitByHash = actualName.split("%");
            final String actualNameWithoutTimeStamp = splitByHash[0];
            return actualNameWithoutTimeStamp;
        }
        return actualName;
    }
    
    @Override
    public HDStore getUsingAlias(final String hdStoreName, final Map<String, Object> properties) {
        if (!Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
            return this.get(hdStoreName, properties);
        }
        if (properties != null && !properties.containsKey("elasticsearch.time_to_live")) {
            return this.get(hdStoreName, properties);
        }
        final IndexOperationsManager indexManager = this.getIndexManager();
        if (!isValidHDStoreName(hdStoreName) || !this.isConnected()) {
            return null;
        }
        final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
        final String aliasName = "search_alias_" + actualName;
        SortedMap<String, AliasOrIndex> map_alias = indexManager.getAliasMapLocally();
        if (map_alias == null || map_alias.isEmpty()) {
            map_alias = indexManager.getAliasMapFromES();
            indexManager.populateIndexCreationTimeMap(map_alias);
        }
        if (!map_alias.containsKey(aliasName)) {
            if (this.checkIfIndexExists(actualName) && properties != null && properties.containsKey("elasticsearch.time_to_live")) {
                properties.put("hd_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
                properties.remove("elasticsearch.time_to_live");
                indexManager.addAliasToIndex(actualName, "search_alias_" + actualName);
                map_alias = indexManager.getAliasMapFromES();
                indexManager.populateIndexCreationTimeMap(map_alias);
            }
            return this.get(hdStoreName, properties);
        }
        final List<String> index_list = indexManager.getIndicesFromAliasName(aliasName, map_alias);
        final String mostRecentIndex = indexManager.getMostRecentIndex(index_list);
        return this.get(mostRecentIndex, properties);
    }
    
    @Override
    public boolean remove(final String hdStoreName) {
        boolean result = false;
        if (isValidHDStoreName(hdStoreName) && this.isConnected()) {
            try {
                final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
                HDStoreManager.logger.info(("Deleting index for hdstore " + hdStoreName + " with index name " + actualName));
                final DeleteIndexRequest request = Requests.deleteIndexRequest(actualName);
                final DeleteIndexResponse response = (DeleteIndexResponse)this.client.admin().indices().delete(request).actionGet();
                result = (response != null && response.isAcknowledged());
                if (!result) {
                    HDStoreManager.logger.warn(("Failed to Delete index for hdstore " + hdStoreName + " with index name " + actualName));
                }
            }
            catch (IndexNotFoundException ignored) {
                HDStoreManager.logger.warn(String.format("HDStore '%s' does not exist", hdStoreName));
            }
        }
        if (result) {
            if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
                final IndexOperationsManager indexManager = this.getIndexManager();
                indexManager.getCreationTimeOfIndicesMap().remove(this.translateName(NameType.HDSTORE, hdStoreName));
                indexManager.updateLocalAliasMap();
            }
            HDStoreManager.logger.info(("Successfully Deleted index for hdstore " + hdStoreName));
            final CheckpointManager checkpointManager = this.getCheckpointManager();
            checkpointManager.remove(hdStoreName);
            HDStoreManager.logger.info(("Successfully Deleted checkpoint index for hdstore " + hdStoreName));
        }
        return result;
    }
    
    @Override
    public boolean removeUsingAlias(final String hdStoreName) {
        if (!Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
            return this.remove(hdStoreName);
        }
        boolean result = true;
        if (!isValidHDStoreName(hdStoreName) || !this.isConnected()) {
            return false;
        }
        final IndexOperationsManager indexManager = this.getIndexManager();
        final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
        final String aliasName = "search_alias_" + actualName;
        indexManager.updateLocalAliasMap();
        if (!indexManager.getAliasMapLocally().containsKey(aliasName)) {
            HDStores.terminateBackgroundTasks(hdStoreName);
            return this.remove(hdStoreName);
        }
        final SortedMap<String, AliasOrIndex> aliasMap = indexManager.getAliasMapLocally();
        for (final String alias_target : aliasMap.keySet()) {
            if (alias_target.equalsIgnoreCase(aliasName)) {
                final AliasOrIndex aliasOrIndex = aliasMap.get(alias_target);
                final Iterator<IndexMetaData> itr_index = aliasOrIndex.getIndices().iterator();
                while (itr_index.hasNext()) {
                    result = (this.remove(itr_index.next().getIndex().getName()) && result);
                }
                HDStores.terminateBackgroundTasks(hdStoreName);
            }
        }
        return result;
    }
    
    @Override
    public HDStore create(String hdStoreName, final Map<String, Object> properties) {
        HDStore result = null;
        if (isValidHDStoreName(hdStoreName) && this.isConnected()) {
            try {
                int indices_count = 1;
                final String actualName = this.translateName(NameType.HDSTORE, hdStoreName);
                final String actualNameWithoutTimeStamp = this.getActualNameWithoutTimeStamp(actualName);
                final Settings.Builder indexSettings = Settings.builder();
                if (properties != null && properties.containsKey("elasticsearch.shards")) {
                    final Integer numberOfShards = Utility.getPropertyInteger(properties, "elasticsearch.shards");
                    if (numberOfShards == null || numberOfShards <= 0) {
                        throw new HDStoreException(String.format("Cannot create HDStore '%s'. Property 'elasticsearch.shards' must be greater than zero", hdStoreName));
                    }
                    indexSettings.put("number_of_shards", (int)numberOfShards);
                }
                if (properties != null && properties.containsKey("elasticsearch.replicas")) {
                    final Integer numberOfReplicas = Utility.getPropertyInteger(properties, "elasticsearch.replicas");
                    if (numberOfReplicas == null || numberOfReplicas < 0) {
                        throw new HDStoreException(String.format("Cannot create HDStore '%s'. Property 'elasticsearch.replicas' must be greater than or equal to zero", hdStoreName));
                    }
                    indexSettings.put("number_of_replicas", (int)numberOfReplicas);
                }
                if (properties != null && properties.containsKey("elasticsearch.refresh_interval")) {
                    final String refreshInterval = Utility.getPropertyString(properties, "elasticsearch.refresh_interval");
                    indexSettings.put("index.refresh_interval", refreshInterval);
                }
                if (properties != null && properties.containsKey("elasticsearch.flush_size")) {
                    final String flushSize = Utility.getPropertyString(properties, "elasticsearch.flush_size");
                    indexSettings.put("index.translog.flush_threshold_size", flushSize);
                }
                if (properties != null && properties.containsKey("elasticsearch.merge_num_threads")) {
                    final String numThreads = Utility.getPropertyString(properties, "elasticsearch.merge_num_threads");
                    indexSettings.put("index.merge.scheduler.max_thread_count", numThreads);
                }
                final String storeType = Utility.getPropertyString(properties, "elasticsearch.storage_type");
                final String indexStoreType = getIndexStoreType(storeType, hdStoreName);
                indexSettings.put("index.store.type", indexStoreType);
                final CreateIndexRequest request = Requests.createIndexRequest(actualName).settings(indexSettings.build());
                final CreateIndexResponse response = (CreateIndexResponse)this.client.admin().indices().create(request).actionGet();
                if (properties != null && properties.containsKey("elasticsearch.time_to_live") && Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
                    final IndexOperationsManager indexManager = this.getIndexManager();
                    properties.put("hd_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
                    properties.remove("elasticsearch.time_to_live");
                    indexManager.addAliasToIndex(actualName, "search_alias_" + actualNameWithoutTimeStamp);
                    indexManager.updateLocalAliasMap();
                    indexManager.populateIndexCreationTimeMap(indexManager.getAliasMapLocally());
                    indices_count = indexManager.getIndicesFromAliasName("search_alias_" + actualNameWithoutTimeStamp, indexManager.getAliasMapLocally()).size();
                }
                if (response != null && response.isAcknowledged() && indices_count == 1) {
                    hdStoreName = this.getActualNameWithoutTimeStamp(hdStoreName);
                    result = new HDStore(this, hdStoreName, properties, actualName);
                }
            }
            catch (ResourceAlreadyExistsException ignored) {
                HDStoreManager.logger.info(String.format("HDStore '%s' already exists", hdStoreName));
            }
        }
        return result;
    }
    
    @Override
    public HDQuery prepareQuery(final JsonNode queryJson) throws HDStoreException {
        this.validateQuery(queryJson);
        final String hdStoreName = queryJson.path("from").get(0).asText();
        final HDStore hdStore = this.getUsingAlias(hdStoreName, null);
        if (hdStore == null) {
            throw new HDStoreMissingException(this.getProviderName(), this.getInstanceName(), hdStoreName);
        }
        com.datasphere.hdstore.elasticsearch.HDQuery result;
        try {
            result = new com.datasphere.hdstore.elasticsearch.HDQuery(queryJson, new HDStore[] { hdStore });
        }
        catch (CapabilityException exception) {
            throw new CapabilityException(this.getProviderName(), exception.capabilityViolated);
        }
        return result;
    }
    
    @Override
    public long delete(final JsonNode queryJson) throws HDStoreException {
        this.validateDelete(queryJson);
        final String hdStoreName = queryJson.path("from").get(0).asText();
        final HDStore hdStore = this.get(hdStoreName, null);
        if (hdStore == null) {
            throw new HDStoreMissingException(this.getProviderName(), this.getInstanceName(), hdStoreName);
        }
        final HDDeleteByQuery statement = new HDDeleteByQuery(queryJson, new HDStore[] { hdStore });
        final HD statementResult = statement.execute().iterator().next();
        return statementResult.get("Result").asLong();
    }
    
    @Override
    public void flush() {
        if (this.isConnected()) {
            final RefreshRequest request = new RefreshRequest(new String[0]);
            this.client.admin().indices().refresh(request).actionGet();
        }
    }
    
    @Override
    public void fsync() {
        if (this.isConnected()) {
            final FlushRequest request = new FlushRequest(new String[0]);
            try {
                this.client.admin().indices().flush(request).actionGet();
            }
            catch (NoNodeAvailableException ex) {}
        }
    }
    
    @Override
    public void close() {
        if (this.client != null) {
            this.fsync();
            this.client.close();
            this.client = null;
        }
    }
    
    @Override
    public void shutdown() {
        if (this.client != null) {
            this.fsync();
            this.shutdownLocalNode();
            this.client.close();
            this.client = null;
        }
    }
    
    private void shutdownLocalNode() {
        if (this.node != null) {
            HDStoreManager.logger.info(String.format("Shutting down local node in cluster '%s'", this.clusterName));
            try {
                this.node.close();
            }
            catch (IOException e) {
                HDStoreManager.logger.error(("Error while shutting down local node: " + this.node.toString()), (Throwable)e);
            }
            this.node = null;
        }
    }
    
    public synchronized Client getClient() {
        if (this.client == null) {
            this.connect();
        }
        return this.client;
    }
    
    private void connect() {
        if ("<Local>".equals(this.clusterName)) {
            HDStoreManager.logger.info("Connecting local client");
            this.client = this.connectLocalClient();
        }
        else if (HDStoreManager.REMOTE_SERVER_CHARACTERS.matcher(this.clusterName).find()) {
            HDStoreManager.logger.info(String.format("Connecting transport client to '%s'", this.clusterName));
            final TransportClient transportClient = (TransportClient)this.connectTransportClient(this.clusterName);
            if (transportClient != null) {
                final List<DiscoveryNode> nodes = (List<DiscoveryNode>)transportClient.connectedNodes();
                if (!nodes.isEmpty()) {
                    this.client = (Client)transportClient;
                }
            }
        }
        else {
            final String nodeName = BaseServer.getServerName();
            HDStoreManager.logger.info(String.format("Connecting node '%s' to cluster '%s'", nodeName, this.clusterName));
            this.client = this.connectNodeClient(nodeName, this.clusterName);
        }
    }
    
    private boolean isConnected() {
        return this.getClient() != null;
    }
    
    public SearchRequestBuilder prepareSearch(final HDStore hdStore, final JsonNode queryJson) {
        final String actualName = hdStore.getActualName();
        final SearchType searchType = SearchType.DEFAULT;
        SearchRequestBuilder result = null;
        if (this.isConnected()) {
            final IndexOperationsManager indexManager = this.getIndexManager();
            indexManager.updateLocalAliasMap();
            if (!indexManager.getAliasMapLocally().containsKey("search_alias_" + actualName)) {
                result = this.client.prepareSearch(new String[] { actualName }).setSearchType(searchType);
            }
            else {
                result = this.client.prepareSearch(new String[] { "search_alias_" + actualName }).setSearchType(searchType);
            }
        }
        return result;
    }
    
    static {
        thisClass = HDStoreManager.class;
        logger = Logger.getLogger((Class)HDStoreManager.thisClass);
        SERVER_NAME_SEPARATOR = Pattern.compile("[,]");
        SERVER_PORT_SEPARATOR = Pattern.compile("[:]");
        REMOTE_SERVER_CHARACTERS = Pattern.compile("[.:]");
        HDSTORE_LOCALE = Locale.getDefault();
        Capabilities = new Capability[] { Capability.SELECT, Capability.INSERT, Capability.DELETE, Capability.PROJECTIONS, Capability.FROM, Capability.WHERE, Capability.ORDER_BY, Capability.GROUP_BY, Capability.LIKE, Capability.REGEX, Capability.BETWEEN, Capability.IN_SET, Capability.AGGREGATION_MIN, Capability.AGGREGATION_MAX, Capability.AGGREGATION_COUNT, Capability.AGGREGATION_SUM, Capability.AGGREGATION_AVG, Capability.DATA_ID, Capability.DATA_TIMESTAMP, Capability.DATA_TIME_TO_LIVE, Capability.DATA_ANY };
    }
}
