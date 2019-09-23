package com.datasphere.runtime;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.eclipse.jetty.server.Handler;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

import com.datasphere.anno.NotSet;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.appmanager.AppManager;
import com.datasphere.appmanager.AppManagerServerLocation;
import com.datasphere.appmanager.EventQueueManager;
import com.datasphere.appmanager.event.Event;
import com.datasphere.classloading.StriimClassLoader;
import com.datasphere.classloading.WALoader;
import com.datasphere.discovery.UdpDiscoveryServer;
import com.datasphere.distribution.HQueue;
import com.datasphere.drop.DropMetaObject;
import com.datasphere.exception.FatalException;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.ServerException;
import com.datasphere.fileSystem.FileSystemBrowserServlet;
import com.datasphere.fileSystem.FileUploaderServlet;
import com.datasphere.historicalcache.Cache;
import com.datasphere.license.LicenseManager;
import com.google.common.collect.Sets;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.persistence.HStore;
import com.datasphere.preview.DataPreviewServlet;
import com.datasphere.proc.BaseProcess;
import com.datasphere.proc.SourceProcess;
import com.datasphere.proc.StreamReader;
import com.datasphere.rest.HealthServlet;
import com.datasphere.rest.MetadataRepositoryServlet;
import com.datasphere.rest.SecurityManagerServlet;
import com.datasphere.rest.StreamInfoServlet;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.channels.ZMQChannel;
import com.datasphere.runtime.compiler.select.RSFieldDesc;
import com.datasphere.runtime.compiler.stmts.DeploymentRule;
import com.datasphere.runtime.compiler.stmts.RecoveryDescription;
import com.datasphere.runtime.components.CQTask;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.IStreamGenerator;
import com.datasphere.runtime.components.IWindow;
import com.datasphere.runtime.components.MultiComponent;
import com.datasphere.runtime.components.Publisher;
import com.datasphere.runtime.components.Restartable;
import com.datasphere.runtime.components.Sorter;
import com.datasphere.runtime.components.Source;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.components.Target;
import com.datasphere.runtime.components.HStoreView;
import com.datasphere.runtime.deployment.Constant;
import com.datasphere.runtime.meta.ImplicitApplicationBuilder;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.LiveObjectViews;
import com.datasphere.runtime.monitor.MonitorModel;
import com.datasphere.runtime.monitor.Monitorable;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.runtime.utils.NetLogger;
import com.datasphere.runtime.window.Window;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.SessionInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.usagemetrics.api.Usage;
import com.datasphere.utility.DeployUtility;
import com.datasphere.utility.GraphUtility;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HDApi;
import com.datasphere.hd.HDApiServlet;
import com.datasphere.hdstore.HDStores;
import com.datasphere.web.MDRepositoryCRUDWebNotificationHandler;
import com.datasphere.web.MetaObjectStatusChangeWebNotificationHandler;
import com.datasphere.web.RMIWebSocket;
import com.datasphere.web.WebServer;
import com.datasphere.web.api.ApplicationHelper;
import com.datasphere.web.api.DashboardAuthenticator;
import com.datasphere.web.api.FileMetadataAPIHelper;
import com.datasphere.web.api.MetadataHelper;
import com.datasphere.wizard.ICDCWizard;
import com.datasphere.wizard.IHazelcastWizard;
import com.datasphere.wizard.TargetWizard;

import javassist.Modifier;

public class Server extends BaseServer implements ShowStreamExecutor, LiveObjectViews, Monitorable, EntryListener<String, Object>, HQueue.Listener, ClientListener
{
    public static final String defaultDeploymentGroupName = "default";
    public static final String SERVER_LIST_LOCK = "ServerList";
    private static Logger logger;
    public static final String verifyPropertyTemplates = "com.striim.verifyPropertyTemplates";
    private final ExecutorService pool;
    private final ScheduledThreadPoolExecutor servicesScheduler;
    private final Map<UUID, HashMap<UUID, Pair<Publisher, Subscriber>>> userQueryRecords;
    private static IMap<UUID, UUID> nodeIDToAuthToken;
    private final AuthToken sessionID;
    private final UUID serverID;
    public static final int shutdownTimeout = 5;
    private AppManager appManager;
    public MetaInfo.Server ServerInfo;
    public HSecurityManager security_manager;
    private IMap<UUID, List<UUID>> serverToDeploymentGroup;
    private final MetaInfo.Initializer initializer;
    public static final String[] azureSQLServerTemplates;
    public static final String[] azureHDInsightTemplates;
    public static final String[] azureStorageTemplates;
    public static final String[] azureEventHubTemplates;
    private static final String[] systemTemplates;
    QueryValidator qv;
    public static volatile Server server;
    private static volatile WebServer webServer;
    private Future<?> discoveryTask;
    
    public static Server getServer() {
        return Server.server;
    }
    
    public AppManager getAppManager() {
        return this.appManager;
    }
    
    public void startAppManager() throws Exception {
        this.appManager = new AppManager(this);
    }
    
    private native void initSimbaServer(final String p0);
    
    private native void setClassLoader(final Object p0);
    
    public Server() {
        this.userQueryRecords = new HashMap<UUID, HashMap<UUID, Pair<Publisher, Subscriber>>>();
        this.qv = null;
        this.discoveryTask = null;
        BaseServer.setMetaDataDbProviderDetails();
        this.initializer = this.startUptheNode();
        this.pool = Executors.newCachedThreadPool(new CustomThreadFactory("BaseServer_WorkingThread"));
        this.servicesScheduler = new ScheduledThreadPoolExecutor(2, new CustomThreadFactory("BaseServer_serviceScheduler", true));
        this.scheduleStatsReporting(statsReporter("main", this.getScheduler()), 5, true);
        this.sessionID = HSecurityManager.TOKEN;
        this.serverID = HazelcastSingleton.getNodeId();
        this.initializeMetaData();
        try {
            this.metadataRepository.initialize();
        }
        catch (MetaDataRepositoryException e) {
            Server.logger.error((Object)e.getMessage());
            System.exit(0);
        }
        Server.baseServer = this;
    }
    
    public void initializeHazelcast() {
        final HazelcastInstance hz = HazelcastSingleton.get();
        hz.getClientService().addClientListener((ClientListener)this);
        final InetSocketAddress adr = hz.getCluster().getLocalMember().getInetSocketAddress();
        Server.logger.info((Object)("Hazelcast cluster has " + hz.getCluster().getMembers().size() + " members"));
        final long serverId = hz.getIdGenerator("#serverids").newId();
        Server.logger.info((Object)("Generated server Id for this server is: " + serverId));
        final String version = Version.getPOMVersionString();
        final int cpuNum = Runtime.getRuntime().availableProcessors();
        final String macAddress = HazelcastSingleton.getMacID();
        this.serverToDeploymentGroup = HazelcastSingleton.get().getMap("#serverToDeploymentGroup");
        Server.nodeIDToAuthToken = HazelcastSingleton.get().getMap("#nodeIDToAuthToken");
        (this.ServerInfo = new MetaInfo.Server()).construct(this.serverID, BaseServer.getServerName(), cpuNum, version, this.initializer, serverId, false);
        this.ServerInfo.setMacAdd(macAddress);
        this.ServerInfo.setUri(NamePolicy.makeKey("Global:" + EntityType.SERVER + ":" + BaseServer.getServerName() + ":" + "1"));
        MDC.put("ServerToken", (Object)this.ServerInfo.name);
        final IMap<String, Object> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
        clusterSettings.addEntryListener((EntryListener)this, true);
        this.initializeMessaging();
    }
    
    public static Runnable statsReporter(final String name, final ThreadPoolExecutor pool) {
        return new Runnable() {
            @Override
            public void run() {
                final long active = pool.getActiveCount();
                final long waiting = pool.getTaskCount();
                final long completed = pool.getCompletedTaskCount();
                final int queueSize = pool.getQueue().size();
                NetLogger.out().println(name + " scheduler tasks: active=" + active + " waiting=" + waiting + " completed=" + completed + " queueSize=" + queueSize);
            }
        };
    }
    
    private void checkVersion() {
        final String curNodeVersion = Version.getPOMVersionString();
        if (Server.logger.isInfoEnabled()) {
            Server.logger.info((Object)("Current node version (read from file " + Version.getBuildPropFileName() + ") : " + curNodeVersion));
        }
        if (persistenceIsEnabled()) {
            if (Version.isConnectedToCompatibleMetaData()) {
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info((Object)("Connected to metadata version : " + Version.getMetaDataVersion()));
                }
            }
            else {
                Server.logger.warn((Object)(">>> Server with version:'" + curNodeVersion + "' is connecting to a incompatible metadata with version:'" + Version.getMetaDataVersion() + "'."));
                System.out.println(">>> Server with version:'" + curNodeVersion + "' is connecting to a incompatible metadata with version:'" + Version.getMetaDataVersion() + "'.");
                System.out.println(">>> initiating shutdown....");
                initiateShutdown(" because, current node  version is:" + curNodeVersion + ", and metadata you trying to connect is of different version:" + Version.getMetaDataVersion());
            }
        }
        final IMap<UUID, MetaInfo.Server> servers = HazelcastSingleton.get().getMap("#servers");
        if (servers != null && servers.size() > 0) {
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)("Checking version before connecting to an existing node/cluster. Number of servers+agents in cluster: " + servers.size()));
            }
            final Set<Map.Entry<UUID, MetaInfo.Server>> set = (Set<Map.Entry<UUID, MetaInfo.Server>>)servers.entrySet();
            final Iterator<Map.Entry<UUID, MetaInfo.Server>> iter = set.iterator();
            final List<MetaInfo.Server> serversList = new ArrayList<MetaInfo.Server>();
            while (iter.hasNext()) {
                final MetaInfo.Server temp = iter.next().getValue();
                if (!temp.isAgent) {
                    serversList.add(temp);
                }
            }
            if (serversList.isEmpty()) {
                return;
            }
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)("Total number of servers in cluster are: " + serversList.size()));
            }
            final MetaInfo.Server firstServer = serversList.get(0);
            final String clusterVersion = firstServer.version;
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)("Matching with the version of first server from cluster running on URI :" + firstServer.webBaseUri));
            }
            if (curNodeVersion.equalsIgnoreCase(clusterVersion)) {
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info((Object)("Joining cluster with version:" + clusterVersion));
                }
            }
            else {
                Server.logger.warn((Object)(">>> Cluster version:'" + clusterVersion + "'  and current node version:'" + curNodeVersion + "'  are NOT compatible."));
                System.out.println(">>> Cluster version '" + clusterVersion + "'  and current node version '" + curNodeVersion + "'  are NOT compatible.");
                System.out.println(">>> initiating shutdown...");
                initiateShutdown(" because, current node version is:" + curNodeVersion + ", and cluster you trying to connect is of different version:" + clusterVersion);
            }
        }
        else if (Server.logger.isInfoEnabled()) {
            Server.logger.info((Object)"First node. No version compatability check necessary.");
        }
    }
    
    private static void initiateShutdown(final String reason) {
        try {
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Server.shutDown(reason);
                    }
                    catch (Exception e) {
                        Server.logger.warn((Object)"Problem shutting down", (Throwable)e);
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(t);
            System.exit(0);
        }
        catch (Exception e) {
            Server.logger.error((Object)"error shutting down server", (Throwable)e);
        }
    }
    
    private void setListeners() {
        final HazelcastIMapListener<String, MetaInfo.MetaObject> listener = new HazelcastIMapListener<String, MetaInfo.MetaObject>() {
            public void entryAdded(final EntryEvent<String, MetaInfo.MetaObject> e) {
                try {
                    Server.this.addedOrUpdated((MetaInfo.MetaObject)e.getValue(), (MetaInfo.MetaObject)e.getOldValue(), false);
                }
                catch (MetaDataRepositoryException e2) {
                    Server.logger.error((Object)e);
                }
            }
            
            public void entryRemoved(final EntryEvent<String, MetaInfo.MetaObject> e) {
                try {
                    Server.this.removed((MetaInfo.MetaObject)e.getOldValue());
                }
                catch (MetaDataRepositoryException e2) {
                    Server.logger.error((Object)e);
                }
            }
            
            public void entryUpdated(final EntryEvent<String, MetaInfo.MetaObject> e) {
                try {
                    Server.this.addedOrUpdated((MetaInfo.MetaObject)e.getValue(), (MetaInfo.MetaObject)e.getOldValue(), true);
                }
                catch (MetaDataRepositoryException e2) {
                    Server.logger.error((Object)e);
                }
            }
        };
        this.metadataRepository.registerListenerForMetaObject(listener);
        final HazelcastIMapListener<UUID, MetaInfo.StatusInfo> statusListener = new StatusListener();
        this.metadataRepository.registerListenerForStatusInfo(statusListener);
        final MessageListener<MetaInfo.ShowStream> showstreamlistener = (MessageListener<MetaInfo.ShowStream>)new MessageListener<MetaInfo.ShowStream>() {
            public void onMessage(final Message<MetaInfo.ShowStream> a) {
                try {
                    Server.this.showStreamStmt((MetaInfo.ShowStream)a.getMessageObject());
                }
                catch (Exception e) {
                    Server.logger.error((Object)e);
                }
            }
        };
        this.metadataRepository.registerListenerForShowStream(showstreamlistener);
    }
    
    private void removeServerFromDeploymentGroup(final Endpoint removedMember) throws MetaDataRepositoryException {
        final List<UUID> deploymentGroups = (List<UUID>)this.serverToDeploymentGroup.get((Object)new UUID(removedMember.getUuid()));
        if (deploymentGroups == null) {
            return;
        }
        for (final UUID deploymentGroup : deploymentGroups) {
            final MetaInfo.DeploymentGroup removedServerDeploymentGroup = (MetaInfo.DeploymentGroup)this.metadataRepository.getMetaObjectByUUID(deploymentGroup, this.sessionID);
            if (removedServerDeploymentGroup == null) {
                Server.logger.warn((Object)("Could not find the deployment group with the UUID : " + deploymentGroup + " as it was already dropped"));
                this.serverToDeploymentGroup.remove((Object)new UUID(removedMember.getUuid()));
                return;
            }
            removedServerDeploymentGroup.removeMember(new UUID(removedMember.getUuid()));
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)("Node " + removedMember.getSocketAddress() + " is removed from deployment group " + removedServerDeploymentGroup));
            }
            this.metadataRepository.putMetaObject(removedServerDeploymentGroup, this.sessionID);
        }
        this.serverToDeploymentGroup.remove((Object)new UUID(removedMember.getUuid()));
    }
    
    public Object adhocCleanup(final String authToken, final UUID queryApplicationUUID) throws MetaDataRepositoryException {
        final LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(authToken, null);
        final Lock lock = LocalLockProvider.getLock(keyForLock);
        lock.lock();
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Object)(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken));
        }
        try {
            final Set<UUID> set = this.getAnyMoreLeftOverAuthTokens();
            set.add(new UUID(authToken));
            if (Server.logger.isDebugEnabled()) {
                Server.logger.debug((Object)(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken));
            }
            for (final UUID clientSessionID : set) {
                if (queryApplicationUUID == null) {
                    final Context ctx = this.createContext(new AuthToken(clientSessionID));
                    final Set<UUID> userQueryApplicationUUIDs = new HashSet<UUID>();
                    final HashMap<UUID, Pair<Publisher, Subscriber>> uuidPairHashMap = this.userQueryRecords.get(clientSessionID);
                    if (uuidPairHashMap != null) {
                        for (final UUID key : uuidPairHashMap.keySet()) {
                            userQueryApplicationUUIDs.add(key);
                        }
                    }
                    final Set<UUID> userQueryUUIDs = new HashSet<UUID>();
                    for (final UUID uuid : userQueryApplicationUUIDs) {
                        final MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)this.getMetaObject(uuid);
                        if (flowMetaObject != null) {
                            if (Server.logger.isDebugEnabled()) {
                                Server.logger.debug((Object)(Thread.currentThread().getName() + " : Calling stop & undeploy on :" + flowMetaObject.name));
                            }
                            final UUID queryMetaObjectUUID = flowMetaObject.getReverseIndexObjectDependencies().iterator().next();
                            userQueryUUIDs.add(queryMetaObjectUUID);
                            try {
                                this.stopUndeployQuery(flowMetaObject, clientSessionID, null);
                            }
                            catch (Exception e) {
                                Server.logger.warn((Object)e.getMessage());
                            }
                        }
                    }
                    for (final UUID queryMetaObjectUUID2 : userQueryUUIDs) {
                        final MetaInfo.Query query = (MetaInfo.Query)this.getObject(queryMetaObjectUUID2);
                        ctx.deleteQuery(query, this.sessionID);
                    }
                    this.userQueryRecords.remove(clientSessionID);
                }
            }
        }
        finally {
            lock.unlock();
            LocalLockProvider.removeLock(keyForLock);
        }
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Object)(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken + " done."));
        }
        return null;
    }
    
    private Set<UUID> getAnyMoreLeftOverAuthTokens() {
        final Set<UUID> set = new HashSet<UUID>();
        for (final UUID uuid : this.userQueryRecords.keySet()) {
            if (!Server.nodeIDToAuthToken.containsValue((Object)uuid) && !HSecurityManager.get().isAuthenticated(new AuthToken(uuid))) {
                set.add(uuid);
            }
        }
        return set;
    }
    
    private int countCaches(final MetaInfo.Flow flow) throws MetaDataRepositoryException {
        int count = flow.getObjects(EntityType.CACHE).size();
        for (final UUID id : flow.getObjects(EntityType.FLOW)) {
            final MetaInfo.Flow subflow = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(id, this.sessionID);
            count += this.countCaches(subflow);
        }
        return count;
    }
    
    protected void setSecMgrAndOther() throws Exception {
        this.security_manager = HSecurityManager.get();
        if (!persistenceIsEnabled()) {
            final MetaInfo.Namespace global = MetaInfo.GlobalNamespace;
            this.putObject(global);
        }
        this.loadModules();
        final String vpt = System.getProperty("com.striim.verifyPropertyTemplates");
        boolean shouldVerifyPT = false;
        if (vpt == null || (vpt != null && (vpt.equals("") || vpt.equalsIgnoreCase("true") || !vpt.equalsIgnoreCase("false")))) {
            shouldVerifyPT = true;
        }
        if (shouldVerifyPT) {
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)"Verifying Property Templates");
            }
            this.loadPropertyTemplates();
        }
        this.initializeDeploymentGroups();
        this.setListeners();
        final ILock lock = HazelcastSingleton.get().getLock("ServerList");
        lock.lock();
        final int allowedCpus = LicenseManager.get().getClusterSize();
        try {
            this.createAdminUserAndRoleSetup();
            Set<MetaInfo.Server> servers = (Set<MetaInfo.Server>)this.metadataRepository.getByEntityType(EntityType.SERVER, this.sessionID);
            if (servers == null) {
                servers = new HashSet<MetaInfo.Server>();
            }
            if (!servers.contains(this.ServerInfo)) {
                servers.add(this.ServerInfo);
            }
            final int numberOfNodesAllowed = LicenseManager.get().getNumberOfNodes();
            if (numberOfNodesAllowed != -1 && servers.size() > numberOfNodesAllowed) {
                final String message = String.format("\nCannot add this HD server to the cluster because it would bring the total number of servers to %d.\nThe current license allows up to %d server(s) in the cluster.\n", servers.size(), numberOfNodesAllowed);
                printf(message);
                lock.unlock();
                System.exit(88);
            }
            printf("Servers in cluster: \n");
            for (final MetaInfo.MetaObject obj : servers) {
                final MetaInfo.Server s = (MetaInfo.Server)obj;
                if (s.isAgent) {
                    continue;
                }
                if (s.uuid.equals((Object)this.ServerInfo.uuid)) {
                    printf("  [this] ");
                }
                else {
                    printf("         ");
                }
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info((Object)(" capacity" + s.numCpus + "\n"));
                }
                printf(s.name + " [" + s.uuid + "] \n");
            }
            final int totalCpus = this.getClusterPhysicalCoreCount(servers);
            if (totalCpus > allowedCpus) {
                final String message = String.format("\nCannot add this HD server to the cluster because it would bring the total number of CPUs to %d.\nThe current license allows up to %d CPU(s) in the cluster.\n", totalCpus, allowedCpus);
                printf(message);
                lock.unlock();
                System.exit(88);
            }
            else if (Server.logger.isInfoEnabled()) {
                Server.logger.info((Object)("Total capacity used " + totalCpus + " of " + allowedCpus + " cpus - " + (allowedCpus - totalCpus) + " remaining\n"));
            }
            this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
        }
        finally {
            lock.unlock();
        }
        this.loadExistingObjects();
        this.insertDefaultKafkaPropertySet();
        HazelcastSingleton.get().getCluster().addMembershipListener((MembershipListener)new ClusterMemberListener());
    }
    
    private void insertDefaultKafkaPropertySet() throws MetaDataRepositoryException {
        MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)this.getObject(EntityType.PROPERTYSET, "Global", "DefaultKafkaProperties");
        if (kpset == null) {
            kpset = new MetaInfo.PropertySet();
            final Map default_properties = new HashMap(2);
            final String zk_address = System.getProperty("com.datasphere.config.zkAddress", "localhost:2181");
            final String broker_address = System.getProperty("com.datasphere.config.brokerAddress", "localhost:9092");
            default_properties.put("zk.address", zk_address);
            default_properties.put("bootstrap.brokers", broker_address);
            default_properties.put("jmx.broker", "localhost:9998");
            kpset.construct("DefaultKafkaProperties", MetaInfo.GlobalNamespace, default_properties);
            this.putObject(kpset);
        }
    }
    
    public int getClusterPhysicalCoreCount(final Set<MetaInfo.Server> serverInstances) {
        int totalCpus = 0;
        final TreeMap<String, Integer> map = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
        for (final MetaInfo.Server server : serverInstances) {
            if (server.isAgent) {
                continue;
            }
            final String key = server.macAdd;
            final Integer val = server.numCpus;
            if (!map.containsKey(key)) {
                map.put(key, val);
            }
            else {
                if (!map.containsKey(key) || val <= map.get(key)) {
                    continue;
                }
                map.put(key, val);
            }
        }
        for (final String key2 : map.keySet()) {
            totalCpus += map.get(key2);
        }
        return totalCpus;
    }
    
    private MetaInfo.Initializer startUptheNode() {
        MetaInfo.Initializer ini = null;
        final boolean persist = persistenceIsEnabled();
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Object)("persist : " + persist));
        }
        final NodeStartUp nsu = new NodeStartUp(persist);
        if (nsu.isNodeStarted()) {
            ini = nsu.getInitializer();
            if (persist) {
                if (Server.logger.isDebugEnabled()) {
                    Server.logger.debug((Object)("Startup details : " + ini.MetaDataRepositoryLocation + " , " + ini.MetaDataRepositoryDBname + " , " + ini.MetaDataRepositoryUname + " , " + ini.MetaDataRepositoryPass));
                }
                HazelcastSingleton.setDBDetailsForMetaDataRepository(ini.MetaDataRepositoryLocation, ini.MetaDataRepositoryDBname, ini.MetaDataRepositoryUname, ini.MetaDataRepositoryPass);
            }
            printf("Current node started in cluster : " + ini.WAClusterName + ", " + (persist ? "with" : "without") + " Metadata Repository \n");
            printf("Registered to: " + ini.CompanyName + "\nProductKey: " + ini.ProductKey + "\n");
            printf("License Key: " + ini.LicenseKey + "\n");
            final LicenseManager lm = LicenseManager.get();
            try {
                lm.setProductKey(ini.CompanyName, ini.ProductKey);
            }
            catch (RuntimeException e) {
                lm.setProductKey(ini.WAClusterName, ini.ProductKey);
            }
            lm.setLicenseKey(ini.LicenseKey);
            if (lm.isExpired()) {
                printf("License " + lm.getExpiry() + "! Please obtain a new license.\n");
                System.exit(99);
            }
            printf("License " + lm.getExpiry() + "\n");
        }
        return ini;
    }
    
    private static Collection<String> getDeploymentGroupsProperty() {
        final Set<String> ret = new HashSet<String>();
        final String dg = System.getProperty("com.datasphere.deploymentGroups");
        if (dg != null) {
            ret.addAll(Arrays.asList(dg.split(",")));
        }
        if (!ret.contains("default")) {
            ret.add("default");
        }
        return ret;
    }
    
    private void initializeDeploymentGroups() throws MetaDataRepositoryException {
        final ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
        lock.lock();
        try {
            final List<UUID> dgList = new ArrayList<UUID>();
            for (final String dgname : getDeploymentGroupsProperty()) {
                MetaInfo.DeploymentGroup dg = this.getDeploymentGroupByName(dgname);
                if (dg == null) {
                    dg = new MetaInfo.DeploymentGroup();
                    dg.construct(dgname);
                    this.putObject(dg);
                }
                dg.addMembers(Collections.singletonList(this.serverID), this.ServerInfo.getId());
                this.ServerInfo.deploymentGroupsIDs.add(dg.uuid);
                this.updateObject(dg);
                dgList.add(dg.uuid);
            }
            this.serverToDeploymentGroup.put(this.serverID, dgList);
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info(("Server " + this.serverID + " is in deployment groups " + this.getDeploymentGroups()));
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    @Override
    public List<String> getDeploymentGroups() {
        final List<String> ret = new ArrayList<String>();
        for (final UUID id : this.ServerInfo.deploymentGroupsIDs) {
            try {
                final MetaInfo.DeploymentGroup dg = this.getDeploymentGroupByID(id);
                ret.add(dg.name);
            }
            catch (ServerException e) {
                Server.logger.error(e, (Throwable)e);
            }
            catch (MetaDataRepositoryException e2) {
                Server.logger.error(e2.getMessage());
            }
        }
        return ret;
    }
    
    private void createAdminUserAndRoleSetup() throws Exception {
        if (this.security_manager.getUser("admin") == null) {
            final String adminPassword = System.getProperty("com.datasphere.config.adminPassword", System.getenv("WA_ADMIN_PASSWORD"));
            boolean verified = false;
            String password;
            if (adminPassword != null && !adminPassword.isEmpty()) {
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info(("Using password sent through system properties : " + adminPassword));
                }
                password = adminPassword;
                verified = true;
            }
            else {
                printf("Required property \"Admin Password\" is undefined\n");
                for (password = ""; password.equals(""); password = ConsoleReader.readPassword("Enter Admin Password: ")) {}
                for (int count = 0; count < 3; ++count) {
                    final String passToString2 = ConsoleReader.readPassword("Re-enter the password : ");
                    if (password.equals(passToString2)) {
                        if (Server.logger.isInfoEnabled()) {
                            Server.logger.info("Matched Password");
                        }
                        verified = true;
                        break;
                    }
                    Server.logger.error("Password did not match");
                    verified = false;
                }
            }
            if (verified) {
                this.createDefaultRoles();
                this.createAdmin(password);
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info("done creating default roles - operator, appadmin, appdev, appuser");
                }
            }
            else {
                System.exit(1);
            }
        }
    }
    
    private void createAdmin(final String password) throws Exception {
        if (this.metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, this.sessionID) == null) {
            final MetaInfo.Role newRole = new MetaInfo.Role();
            newRole.construct(MetaInfo.GlobalNamespace, "admin");
            newRole.grantPermission(new ObjectPermission("*"));
            this.putObject(newRole);
        }
        if (this.security_manager.getUser("admin") == null) {
            final MetaInfo.Role r = (MetaInfo.Role)this.getObject(EntityType.ROLE, "Global", "admin");
            final List<String> lrole = new ArrayList<String>();
            lrole.add(r.getRole());
            MetaInfo.User newuser = new MetaInfo.User();
            newuser.construct("admin", password);
            newuser.setDefaultNamespace("admin");
            this.security_manager.addUser(newuser, this.sessionID);
            final MetaInfo.Namespace newnamespace = (MetaInfo.Namespace)this.getObject(EntityType.NAMESPACE, "Global", "admin");
            if (newnamespace == null) {
                final AuthToken temptoken = HSecurityManager.TOKEN;
                final Context ctx = this.createContext(temptoken);
                final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
                final SessionInfo value = new SessionInfo("admin", System.currentTimeMillis());
                authTokens.put(temptoken, value);
                ctx.putNamespace(false, "admin");
                authTokens.remove(temptoken);
                this.security_manager.refreshInternalMaps(null);
            }
            newuser = this.security_manager.getUser("admin");
            this.security_manager.grantUserRoles(newuser.getUserId(), lrole, this.sessionID);
        }
    }
    
    private void createDefaultRoles() throws Exception {
        if (this.security_manager.getRole("Global:appadmin") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appadmin");
            role.grantPermissions(getDefaultAppAdminPermissions());
            this.security_manager.addRole(role, this.sessionID);
        }
        if (this.security_manager.getRole("Global:appdev") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appdev");
            role.grantPermissions(getDefaultDevPermissions());
            this.security_manager.addRole(role, this.sessionID);
        }
        if (this.security_manager.getRole("Global:appuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appuser");
            role.grantPermissions(getDefaultEndUserPermissions());
            this.security_manager.addRole(role, this.sessionID);
        }
        if (this.security_manager.getRole("Global:systemuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "systemuser");
            role.grantPermissions(getDefaultSystemUserPermissions());
            this.security_manager.addRole(role, this.sessionID);
        }
        if (this.security_manager.getRole("Global:uiuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "uiuser");
            role.grantPermissions(getDefaultUIUserPermissions());
            this.security_manager.addRole(role, this.sessionID);
        }
    }
    
    private static List<ObjectPermission> getDefaultSystemUserPermissions() throws Exception {
        final List<ObjectPermission> defaultUserPermissions = new ArrayList<ObjectPermission>();
        final Set<String> domains = new HashSet<String>();
        final Set<ObjectPermission.Action> actions = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes = new HashSet<ObjectPermission.ObjectType>();
        final Set<String> names = new HashSet<String>();
        final Set<String> domains2 = new HashSet<String>();
        final Set<ObjectPermission.Action> actions2 = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes2 = new HashSet<ObjectPermission.ObjectType>();
        domains2.add("Global");
        actions2.add(ObjectPermission.Action.select);
        actions2.add(ObjectPermission.Action.read);
        objectTypes2.add(ObjectPermission.ObjectType.propertytemplate);
        objectTypes2.add(ObjectPermission.ObjectType.deploymentgroup);
        objectTypes2.add(ObjectPermission.ObjectType.type);
        final ObjectPermission readPropTemplatesPerm = new ObjectPermission(domains2, actions2, objectTypes2, null);
        defaultUserPermissions.add(readPropTemplatesPerm);
        return defaultUserPermissions;
    }
    
    private static List<ObjectPermission> getDefaultUIUserPermissions() throws Exception {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p1 = new ObjectPermission("*:*:dashboard_ui,apps_ui,sourcepreview_ui,monitor_ui:*");
        permissions.add(p1);
        return permissions;
    }
    
    private static List<ObjectPermission> getDefaultAppAdminPermissions() throws Exception {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p1 = new ObjectPermission("Global:create:namespace:*");
        permissions.add(p1);
        final ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
        permissions.add(p2);
        return permissions;
    }
    
    private static List<ObjectPermission> getDefaultDevPermissions() throws Exception {
        final List<ObjectPermission> allPermissions = getDefaultEndUserPermissions();
        final ObjectPermission p1 = new ObjectPermission("*:create:namespace:*");
        allPermissions.add(p1);
        return allPermissions;
    }
    
    private static List<ObjectPermission> getDefaultEndUserPermissions() throws Exception {
        final List<ObjectPermission> allPermissions = new ArrayList<ObjectPermission>();
        final Set<String> domains = new HashSet<String>();
        final Set<ObjectPermission.Action> actions = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes = new HashSet<ObjectPermission.ObjectType>();
        domains.add("Global");
        actions.add(ObjectPermission.Action.select);
        actions.add(ObjectPermission.Action.read);
        final ObjectPermission.ObjectType[] values;
        final ObjectPermission.ObjectType[] allObjectTypes = values = ObjectPermission.ObjectType.values();
        for (final ObjectPermission.ObjectType type : values) {
            if (!type.equals(ObjectPermission.ObjectType.user)) {
                objectTypes.add(type);
            }
        }
        final ObjectPermission globalPermissions = new ObjectPermission(domains, actions, objectTypes, null);
        allPermissions.add(globalPermissions);
        return allPermissions;
    }
    
    public void loadExistingObjects() throws Exception {
        this.initMonitoringApp();
        final MDRepository md = this.metadataRepository;
        if (md == null) {
            return;
        }
        final Integer maxId = md.getMaxClassId();
        assert maxId != null;
        if (maxId > -1) {
            final WALoader waLoader = WALoader.get();
            waLoader.setMaxClassId(maxId);
        }
        final Set<MetaInfo.Namespace> namespaces = (Set<MetaInfo.Namespace>)md.getByEntityType(EntityType.NAMESPACE, this.sessionID);
        final Set<UUID> allApplications = new LinkedHashSet<UUID>();
        for (final MetaInfo.Namespace namespace : namespaces) {
            if (Server.logger.isInfoEnabled()) {
                Server.logger.info(("Loading objects for namespace: " + namespace.name));
            }
            final Set<MetaInfo.MetaObject> objs = MetadataRepository.getINSTANCE().getByNameSpace(namespace.name, this.sessionID);
            if (objs != null) {
                for (final MetaInfo.MetaObject obj : objs) {
                    if (obj.type == EntityType.TYPE) {
                        if (Server.logger.isInfoEnabled()) {
                            new StringBuilder().append(obj.type).append(" --> ").append(obj.uuid).append(" ; ").append(namespace.name).append(":").append(obj.type).append(":").append(obj.name).append(" ; version number ").append(obj.version).toString();
                        }
                        this.added(obj);
                    }
                }
                for (final MetaInfo.MetaObject obj : objs) {
                    if (obj.type == EntityType.STREAM) {
                        if (Server.logger.isInfoEnabled()) {
                            new StringBuilder().append(obj.type).append(" --> ").append(obj.uuid).append(" ; ").append(namespace.name).append(":").append(obj.type).append(":").append(obj.name).append(" ; version number ").append(obj.version).toString();
                        }
                        if (((MetaInfo.Stream)obj).pset == null) {
                            continue;
                        }
                        KafkaStreamUtils.getPropertySet((MetaInfo.Stream)obj);
                        this.metadataRepository.updateMetaObject(obj, HSecurityManager.TOKEN);
                    }
                }
                for (final MetaInfo.MetaObject obj : objs) {
                    if (obj.type == EntityType.HDSTORE) {
                        if (Server.logger.isInfoEnabled()) {
                            new StringBuilder().append(obj.type).append(" --> ").append(obj.uuid).append(" ; ").append(namespace.name).append(":").append(obj.type).append(":").append(obj.name).append(" ; version number ").append(obj.version).toString();
                        }
                        this.added(obj);
                    }
                }
                for (final MetaInfo.MetaObject obj : objs) {
                    if (obj.type != EntityType.TYPE && obj.type != EntityType.STREAM && obj.type != EntityType.APPLICATION && obj.type != EntityType.HDSTORE) {
                        if (Server.logger.isInfoEnabled()) {
                            new StringBuilder().append(obj.type).append(" --> ").append(obj.uuid).append(" ; ").append(namespace.name).append(":").append(obj.type).append(":").append(obj.name).append(" ; version number ").append(obj.version).toString();
                        }
                        this.added(obj);
                    }
                }
                for (final MetaInfo.MetaObject obj : objs) {
                    DropMetaObject.addToRecentVersion(obj.nsName, obj.uuid);
                    if (obj.type == EntityType.APPLICATION) {
                        if (!MonitorModel.monitorIsEnabled()) {
                            final MetaInfo.Flow flow = (MetaInfo.Flow)obj;
                            if (flow.getFullName().equals("Global.MonitoringSourceApp") || flow.getFullName().equals("Global.MonitoringProcessApp")) {
                                if (Server.logger.isInfoEnabled()) {
                                    Server.logger.info("HD Monitor is not enabled");
                                    continue;
                                }
                                continue;
                            }
                        }
                        if (Server.logger.isInfoEnabled()) {
                            new StringBuilder().append(obj.type).append(" --> ").append(obj.uuid).append(" ; ").append(namespace.name).append(":").append(obj.type).append(":").append(obj.name).append(" ; version number ").append(obj.version).toString();
                        }
                        allApplications.add(obj.getUuid());
                        this.added(obj);
                    }
                }
            }
        }
    }
    
    private LinkedHashSet<UUID> calculateApplicationOrdering(final Set<UUID> allApplications) throws Exception {
        final Map<UUID, Set<UUID>> dependentApps = new HashMap<UUID, Set<UUID>>();
        final Map<UUID, Set<UUID>> objectDependentOfApps = new HashMap<UUID, Set<UUID>>();
        for (final UUID applicationUUID : allApplications) {
            final MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)this.getMetaObject(applicationUUID);
            if (flowMetaObject == null) {
                Server.logger.warn(("Failed to calcualte application ordering for appId " + applicationUUID + " because flowMetaObject does not exist for this flow"));
            }
            else {
                Set<UUID> appDeepDependencies;
                try {
                    appDeepDependencies = flowMetaObject.getDeepDependencies();
                }
                catch (FatalException e) {
                    Server.logger.info("Problem occured in finding application dependencies, skipping application ordering.", (Throwable)e);
                    throw e;
                }
                for (final UUID uuid : appDeepDependencies) {
                    if (objectDependentOfApps.get(uuid) == null) {
                        final Set<UUID> appSet = new HashSet<UUID>();
                        appSet.add(flowMetaObject.uuid);
                        objectDependentOfApps.put(uuid, appSet);
                    }
                    else {
                        objectDependentOfApps.get(uuid).add(flowMetaObject.uuid);
                    }
                }
            }
        }
        for (final Map.Entry<UUID, Set<UUID>> objectDependencyMapEntry : objectDependentOfApps.entrySet()) {
            if (objectDependencyMapEntry.getValue().size() > 1) {
                for (final UUID application : objectDependencyMapEntry.getValue()) {
                    final MetaInfo.Flow flowMetaObject2 = (MetaInfo.Flow)this.getObject(application);
                    final Set<UUID> flowObjects = flowMetaObject2.getAllObjects();
                    if (flowObjects.contains(objectDependencyMapEntry.getKey())) {
                        final List<UUID> adjacencyList = new ArrayList<UUID>();
                        adjacencyList.addAll(objectDependencyMapEntry.getValue());
                        adjacencyList.remove(flowMetaObject2.getUuid());
                        for (final UUID adjacentVertexUUID : adjacencyList) {
                            final UUID vertexFirst = flowMetaObject2.getUuid();
                            final UUID vertexSecond = adjacentVertexUUID;
                            final MetaInfo.Flow secondVertexApplication = (MetaInfo.Flow)this.getObject(vertexSecond);
                            final Set<UUID> secondVertexApplicationObjects = secondVertexApplication.getAllObjects();
                            if (secondVertexApplicationObjects.contains(objectDependencyMapEntry.getKey())) {
                                continue;
                            }
                            if (!dependentApps.containsKey(vertexFirst)) {
                                dependentApps.put(vertexFirst, new LinkedHashSet<UUID>());
                            }
                            if (!dependentApps.containsKey(vertexSecond)) {
                                dependentApps.put(vertexSecond, new LinkedHashSet<UUID>());
                            }
                            dependentApps.get(vertexFirst).add(vertexSecond);
                        }
                    }
                }
            }
        }
        final LinkedHashSet<UUID> newOrder = new LinkedHashSet<UUID>();
        newOrder.addAll(GraphUtility.topologicalSort(dependentApps));
        for (final UUID uuid2 : allApplications) {
            if (!newOrder.contains(uuid2)) {
                newOrder.add(uuid2);
            }
        }
        return newOrder;
    }
    
    private Set<URL> getListOfPropTempURLs() {
        final String homeList = System.getProperty("com.datasphere.platform.home");
        String[] platformHome = new String[2];
        if (homeList != null && homeList.contains(";")) {
            platformHome = homeList.split(";");
        }
        else {
            platformHome[0] = homeList;
        }
        final Set<URL> result = Sets.newHashSet();
        for (final String aPlatformHome : platformHome) {
            File platformHomeFile;
            if (aPlatformHome == null) {
                platformHomeFile = new File(".");
            }
            else {
                platformHomeFile = new File(aPlatformHome);
            }
            String platformHomeAbs;
            try {
                platformHomeAbs = platformHomeFile.getCanonicalPath();
            }
            catch (IOException e) {
                platformHomeAbs = platformHomeFile.getAbsolutePath();
            }
            final ClassLoader[] classLoaders;
            final ClassLoader[] loaders = classLoaders = ClasspathHelper.classLoaders(new ClassLoader[0]);
            for (ClassLoader classLoader : classLoaders) {
                while (classLoader != null) {
                    if (classLoader instanceof URLClassLoader) {
                        final URL[] urls = ((URLClassLoader)classLoader).getURLs();
                        if (urls != null) {
                            for (final URL url : urls) {
                                if (url.getPath().contains(platformHomeAbs)) {
                                    result.add(url);
                                }
                            }
                        }
                    }
                    classLoader = classLoader.getParent();
                }
            }
        }
        return result;
    }
    
    public boolean isTemplateAllowed(final String templateName) {
        return ArrayUtils.contains((Object[])Server.systemTemplates, templateName) || LicenseManager.get().allowsAllTemplates() || (LicenseManager.get().allowsOption(LicenseManager.Option.AzureSQLServer) && ArrayUtils.contains((Object[])Server.azureSQLServerTemplates, templateName)) || (LicenseManager.get().allowsOption(LicenseManager.Option.AzureHDInsight) && ArrayUtils.contains((Object[])Server.azureHDInsightTemplates, templateName)) || (LicenseManager.get().allowsOption(LicenseManager.Option.AzureStorage) && ArrayUtils.contains((Object[])Server.azureStorageTemplates, templateName)) || (LicenseManager.get().allowsOption(LicenseManager.Option.AzureEventHub) && ArrayUtils.contains((Object[])Server.azureEventHubTemplates, templateName));
    }
    
    private void loadClasses(final Collection<Class<?>> annotatedClasses) {
        Lock propTemplateLock = null;
        try {
            propTemplateLock = (Lock)HazelcastSingleton.get().getLock("propTemplateLock");
            propTemplateLock.lock();
            for (final Class<?> c : annotatedClasses) {
                try {
                    final PropertyTemplate pt = c.getAnnotation(PropertyTemplate.class);
                    if (pt == null) {
                        continue;
                    }
                    String name = pt.name();
                    if (pt.version() != null && !pt.version().equalsIgnoreCase("0.0.0")) {
                        name = name + "_" + pt.version();
                    }
                    if (!this.isTemplateAllowed(name)) {
                        continue;
                    }
                    final PropertyTemplateProperty[] ptp = pt.properties();
                    final Map<String, MetaInfo.PropertyDef> props = Factory.makeMap();
                    for (final PropertyTemplateProperty val : ptp) {
                        final MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
                        def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
                        props.put(val.name(), def);
                    }
                    this.genDefaultClasses(pt);
                    MetaInfo.PropertyTemplateInfo pti = this.getPropertyTemplateByNameAndVersion(pt.name(), pt.version());
                    boolean isUnique = false;
                    if (pti != null) {
                        final Map<String, MetaInfo.PropertyDef> defCurrent = pti.propertyMap;
                        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : props.entrySet()) {
                            if (!entry.getValue().equals(defCurrent.get(entry.getKey()))) {
                                isUnique = true;
                                break;
                            }
                        }
                        if (!pt.type().equals(pti.getAdapterType()) || !pt.name().equals(pti.getName())) {
                            isUnique = true;
                        }
                        if (pt.requiresFormatter() != pti.isRequiresFormatter()) {
                            isUnique = true;
                        }
                        if (pt.requiresParser() != pti.isRequiresParser()) {
                            isUnique = true;
                        }
                        if (!pt.inputType().getName().equals(pti.getInputClassName())) {
                            isUnique = true;
                        }
                        if (!pt.outputType().getName().equals(pti.getOutputClassName())) {
                            isUnique = true;
                        }
                        if (!pt.version().equals(pti.getAdapterVersion())) {
                            isUnique = true;
                        }
                        if (pt.isParallelizable() != pti.isParallelizable) {
                            isUnique = true;
                        }
                    }
                    else {
                        isUnique = true;
                    }
                    if (!isUnique) {
                        continue;
                    }
                    pti = new MetaInfo.PropertyTemplateInfo();
                    pti.construct(pt.name(), pt.type(), props, pt.inputType().getName(), pt.outputType().getName(), c.getName(), pt.requiresParser(), pt.requiresFormatter(), pt.version(), pt.isParallelizable());
                    this.putObject(pti);
                }
                catch (Exception e) {
                    Server.logger.error(("Problem loading Property Template for class: " + c.getCanonicalName() + ". Will continue server start up anyways, but the adapter associated with " + c.getCanonicalName() + " will not be usable."), (Throwable)e);
                }
            }
        }
        finally {
            if (propTemplateLock != null) {
                propTemplateLock.unlock();
            }
        }
    }
    
    private void loadModules() {
        final StriimClassLoader scl = (StriimClassLoader)Thread.currentThread().getContextClassLoader();
        try {
            scl.scanModulePath();
        }
        catch (IOException ioe) {
            Server.logger.warn(("Unable to load any of the modules from modulepath: " + ioe.getMessage()), (Throwable)ioe);
        }
        final Set<Class<?>> moduleServices = scl.getAllServicesOfModules();
        this.loadClasses(moduleServices);
    }
    
    private void loadPropertyTemplates() {
        final Set<URL> propTemps = this.getListOfPropTempURLs();
        final Reflections refs = new Reflections(propTemps.toArray());
        Set<Class<?>> annotatedClasses = new HashSet<Class<?>>();
        try {
            annotatedClasses = (Set<Class<?>>)refs.getTypesAnnotatedWith((Class)PropertyTemplate.class);
        }
        catch (Exception e) {
            Server.logger.error("Encountered an issue while trying to get classes annotated with PropertyTemplate.class, hence sever will crash", (Throwable)e);
            System.exit(1);
        }
        this.loadClasses(annotatedClasses);
    }
    
    private void genDefaultClasses(final PropertyTemplate pt) throws MetaDataRepositoryException {
        final Map<String, String> fields = new LinkedHashMap<String, String>();
        final Class<?> inputType = pt.inputType();
        final Class<?> outputType = pt.outputType();
        MetaInfo.Type type = null;
        if (!inputType.equals(NotSet.class)) {
            final Field[] declaredFields;
            final Field[] cFields = declaredFields = inputType.getDeclaredFields();
            for (final Field f : declaredFields) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            if (this.getObject(EntityType.TYPE, "Global", inputType.getSimpleName()) == null) {
                type = new MetaInfo.Type();
                type.construct("Global." + inputType.getSimpleName(), MetaInfo.GlobalNamespace, inputType.getName(), fields, null, false);
            }
        }
        else {
            final Field[] declaredFields2;
            final Field[] cFields = declaredFields2 = outputType.getDeclaredFields();
            for (final Field f : declaredFields2) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            if (this.getObject(EntityType.TYPE, "Global", outputType.getSimpleName()) == null) {
                type = new MetaInfo.Type();
                type.construct("Global." + outputType.getSimpleName(), MetaInfo.GlobalNamespace, outputType.getName(), fields, null, false);
            }
        }
        if (type != null) {
            this.putObject(type);
        }
    }
    
    @Override
    public <T extends MetaInfo.MetaObject> T getObjectInfo(final UUID uuid, final EntityType type) throws ServerException, MetaDataRepositoryException {
        final MetaInfo.MetaObject o = this.getObject(uuid);
        if (o != null && o.type == type) {
            return (T)o;
        }
        throw new ServerException("cannot find metadata for " + type.name() + " " + uuid);
    }
    
    @Override
    public ExecutorService getThreadPool() {
        return this.pool;
    }
    
    public ScheduledFuture<?> scheduleStatsReporting(final Runnable task, final int period, final boolean optional) {
        return optional ? null : this.servicesScheduler.scheduleAtFixedRate(task, period, period, TimeUnit.SECONDS);
    }
    
    @Override
    public boolean inDeploymentGroup(final UUID deploymentGroupID) {
        return this.ServerInfo.deploymentGroupsIDs.contains(deploymentGroupID);
    }
    
    boolean checkIfMetaObjectExists(final UUID uuid) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByUUID(uuid, this.sessionID) != null;
    }
    
    public void added(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        DropMetaObject.addToRecentVersion(obj.nsName, obj.uuid);
        this.addedOrUpdated(obj, null, false);
    }
    
    public void addedOrUpdated(final MetaInfo.MetaObject obj, final MetaInfo.MetaObject oldObj, final boolean updated) throws MetaDataRepositoryException {
        switch (obj.type) {
            case TYPE: {
                final MetaInfo.Type type = (MetaInfo.Type)obj;
                if (!type.getMetaInfoStatus().isDropped()) {
                    try {
                        type.generateClass();
                    }
                    catch (Exception e) {
                        Server.logger.error(e);
                    }
                }
                break;
            }
            case DG: {
                final MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)obj;
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info(("Got update to deploymentgroup: " + dg.uri + " " + dg));
                }
                if (dg.isAutomaticDG()) {
                    break;
                }
                final ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
                lock.lock();
                try {
                    final List<UUID> dgs = (List<UUID>)this.serverToDeploymentGroup.get(this.serverID);
                    assert dgs != null;
                    final String serverName = BaseServer.getServerName();
                    if (serverName != null && dg.configuredMembers.contains(serverName)) {
                        if (Server.logger.isInfoEnabled()) {
                            Server.logger.info(("Adding server " + serverName + " to dg " + dg.name));
                        }
                        if (this.ServerInfo.deploymentGroupsIDs.add(dg.uuid)) {
                            dgs.add(dg.uuid);
                            final MetaInfo.DeploymentGroup dgLatest = this.getDeploymentGroupByName(dg.getName());
                            dgLatest.addMembers(Collections.singletonList(this.serverID), this.ServerInfo.getId());
                            this.updateObject(dgLatest);
                            this.serverToDeploymentGroup.put(this.serverID, dgs);
                            this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
                            if (Server.logger.isInfoEnabled()) {
                                Server.logger.info(("Added DG " + dg.name + "\nCurrent deployment groups: " + this.getDeploymentGroups()));
                            }
                        }
                    }
                    else if (this.ServerInfo.deploymentGroupsIDs.remove(dg.uuid)) {
                        dgs.remove(dg.uuid);
                        this.serverToDeploymentGroup.put(this.serverID, dgs);
                        this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
                        if (Server.logger.isInfoEnabled()) {
                            Server.logger.info(("Removed DG " + dg.name + "\nCurrent deployment groups: " + this.getDeploymentGroups()));
                        }
                    }
                }
                finally {
                    lock.unlock();
                }
                break;
            }
            case ROLE: {
                this.security_manager.refreshInternalMaps(null);
                break;
            }
            case USER: {
                this.security_manager.refreshInternalMaps(((MetaInfo.User)obj).name);
                break;
            }
            case HDSTORE: {
                final MetaInfo.HDStore hdStore = (MetaInfo.HDStore)obj;
                if (!hdStore.getMetaInfoStatus().isDropped()) {
                    try {
                        hdStore.generateClasses();
                    }
                    catch (Exception e) {
                        Server.logger.error(("Failed to generate classes for HD Store: " + hdStore.getFullName()), (Throwable)e);
                    }
                    break;
                }
                break;
            }
        }
    }
    
    public void removed(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        switch (obj.type) {
            case DG: {
                final MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)obj;
                if (Server.logger.isInfoEnabled()) {
                    Server.logger.info(("Got removal request for deploymentgroup: " + dg.uri + " " + dg));
                }
                if (dg.isAutomaticDG()) {
                    break;
                }
                final ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
                lock.lock();
                try {
                    final List<UUID> dgs = (List<UUID>)this.serverToDeploymentGroup.get(this.serverID);
                    assert dgs != null;
                    if (this.ServerInfo.deploymentGroupsIDs.remove(dg.uuid)) {
                        dgs.remove(dg.uuid);
                        this.serverToDeploymentGroup.put(this.serverID, dgs);
                        this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
                        Server.logger.info(("Removed DG " + dg.name + "\nCurrent deployment groups: " + this.getDeploymentGroups()));
                    }
                }
                finally {
                    lock.unlock();
                }
                break;
            }
        }
    }
    
    private void addAlertQueue(final String channelName) {
        HQueue.getQueue(channelName);
    }
    
    public HStore createHDStore(final MetaInfo.HDStore obj) throws Exception {
        final HStore was = HStore.get(obj.uuid, this);
        this.putOpenObject(was);
        return was;
    }
    
    public HStoreView createWAStoreView(final MetaInfo.WAStoreView obj) throws Exception {
        final HStore hStore = (HStore)this.getOpenObject(obj.wastoreID);
        final HStoreView wasview = new HStoreView(obj, hStore, this);
        this.putOpenObject(wasview);
        return wasview;
    }
    
    public void removeDeployedObject(final FlowComponent obj) throws Exception {
        if (obj.getMetaType() == EntityType.SOURCE) {
            try {
                final FlowComponent so = this.getOpenObject(obj.getMetaID());
                if (so != null) {
                    final Source src = (Source)so;
                    src.getAdapter().onUndeploy();
                }
            }
            catch (Exception e) {
                Server.logger.error(("Failed to call unDeploy on Source process for " + obj.getMetaName()), (Throwable)e);
            }
        }
        final FlowComponent o = this.removeDeployedObjectById(obj.getMetaID());
        if (o == null) {
            this.closeOpenObject(obj);
        }
        else if (o != obj) {
            throw new RuntimeException("system error: duplicate object " + obj.metaToString());
        }
    }
    
    public FlowComponent removeDeployedObjectById(final UUID id) throws Exception {
        synchronized (this.openObjects) {
            final FlowComponent o = this.openObjects.remove(id);
            if (o != null) {
                this.closeOpenObject(o);
            }
            return o;
        }
    }
    
    public Stream createStream(final UUID uuid, final Flow flow) throws Exception {
        final MetaInfo.Stream si = this.getStreamInfo(uuid);
        if (si != null) {
            final StreamObjectFac fac = new StreamObjectFac() {
                @Override
                public FlowComponent create() throws Exception {
                    final Stream s = new Stream(si, Server.this, flow);
                    return s;
                }
            };
            final FlowComponent stream_component = this.putOpenObjectIfNotExists(uuid, fac);
            return (Stream)stream_component;
        }
        return null;
    }
    
    @Override
    public Stream getStream(final UUID streamId, final Flow flow) throws Exception {
        final StreamObjectFac fac = new StreamObjectFac() {
            @Override
            public FlowComponent create() throws Exception {
                final MetaInfo.Stream si = Server.this.getStreamInfo(streamId);
                final Stream s = new Stream(si, Server.this, flow);
                return s;
            }
        };
        return (Stream)this.putOpenObjectIfNotExists(streamId, fac);
    }
    
    public IWindow createWindow(final MetaInfo.Window info) throws Exception {
        final IWindow w = new Window(info, this);
        this.putOpenObject(w);
        return w;
    }
    
    public CQTask createCQ(final MetaInfo.CQ info) throws Exception {
        final CQTask ct = new CQTask(info, this);
        this.putOpenObject(ct);
        return ct;
    }
    
    public Sorter createSorter(final MetaInfo.Sorter info) throws Exception {
        final Sorter s = new Sorter(info, this);
        this.putOpenObject(s);
        return s;
    }
    
    public Target createTarget(final MetaInfo.Target targetInfo) throws Exception {
        return this.createTarget(targetInfo, true);
    }
    
    public Target createTarget(final MetaInfo.Target targetInfo, final boolean putInOpenObjects) throws Exception {
        try {
            final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(targetInfo.adapterClassName);
            final BaseProcess bp = (BaseProcess)adapterFactory.newInstance();
            final Target t = new Target(bp, targetInfo, this);
            t.onDeploy();
            if (putInOpenObjects) {
                this.putOpenObject(t);
            }
            return t;
        }
        catch (ClassNotFoundException e) {
            throw new ServerException("cannot create adapter " + targetInfo.adapterClassName, e);
        }
    }
    
    public Cache createCache(final MetaInfo.Cache targetInfo) throws Exception {
        try {
            synchronized (this.openObjects) {
                BaseProcess bp = null;
                final MetaInfo.Type t = (MetaInfo.Type)this.getObject(targetInfo.typename);
                targetInfo.reader_properties.put("modalname", t.className);
                if (targetInfo.adapterClassName != null) {
                    final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(targetInfo.adapterClassName);
                    targetInfo.reader_properties.put("noeofwait", true);
                    bp = (BaseProcess)adapterFactory.newInstance();
                }
                final Cache cache = new Cache(targetInfo, this, bp);
                this.putOpenObject(cache);
                return cache;
            }
        }
        catch (ClassNotFoundException e) {
            throw new ServerException("cannot create adapter " + targetInfo.adapterClassName, e);
        }
    }
    
    public IStreamGenerator createStreamGen(final MetaInfo.StreamGenerator g) throws Exception {
        synchronized (this.openObjects) {
            try {
                final Class<?> klass = ClassLoader.getSystemClassLoader().loadClass(g.className);
                final Constructor<?> ctor = klass.getConstructor(MetaInfo.StreamGenerator.class, BaseServer.class);
                final IStreamGenerator gen = (IStreamGenerator)ctor.newInstance(g, this);
                this.putOpenObject(gen);
                return gen;
            }
            catch (ClassNotFoundException e) {
                throw new ServerException("cannot find generator class " + g.className, e);
            }
            catch (ClassCastException e2) {
                throw new ServerException("class " + g.className + " does not support interface IStreamGenerator", e2);
            }
        }
    }
    
    private void getStreamsInObject(final UUID id, final Map<UUID, MetaInfo.Stream> result) throws Exception {
        final MetaInfo.MetaObject obj = this.getObject(id);
        if (obj != null) {
            if (obj instanceof MetaInfo.Stream) {
                result.put(id, (MetaInfo.Stream)obj);
            }
            else {
                for (final UUID dep : obj.getDependencies()) {
                    this.getStreamsInObject(dep, result);
                }
            }
        }
    }
    
    @Override
    public void shutdown() {
        Server.logger.warn("Shutting down Striim Server\n");
        if (this.discoveryTask != null) {
            this.discoveryTask.cancel(true);
        }
        HDStores.shutdown();
        this.servicesScheduler.shutdownNow();
        this.pool.shutdown();
        try {
            if (!this.pool.awaitTermination(5L, TimeUnit.SECONDS)) {
                this.pool.shutdownNow();
                if (!this.pool.awaitTermination(60L, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate: " + this.pool);
                }
            }
        }
        catch (InterruptedException ie) {
            this.pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        super.shutdown();
        if (Server.logger.isInfoEnabled()) {
            Server.logger.info("Striim Server shutdown finished");
        }
    }
    
    @Override
    public ZMQChannel getDistributedChannel(final Stream owner) {
        return new ZMQChannel(this, owner);
    }
    
    public Flow createFlow(final MetaInfo.Flow flowInfo, final Flow parent) throws Exception {
        final Flow flow = new Flow(flowInfo, this, parent);
        this.putOpenObject(flow);
        return flow;
    }
    
    public Source createSource(final MetaInfo.Source sourceInfo) throws Exception {
        try {
            final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(sourceInfo.adapterClassName);
            final String namespace = sourceInfo.getNsName();
            final SourceProcess bp = (SourceProcess)adapterFactory.newInstance();
            if (bp instanceof StreamReader) {
                final StreamReader x = (StreamReader)bp;
                x.setNamespace(namespace);
            }
            final Source src = new Source(bp, sourceInfo, this);
            this.putOpenObject(src);
            return src;
        }
        catch (ClassNotFoundException e) {
            throw new ServerException("cannot create adapter " + sourceInfo.adapterClassName, e);
        }
    }
    
    protected MetaInfo.PropertyTemplateInfo getAdapterProperties(final String name) throws MetaDataRepositoryException {
        return (MetaInfo.PropertyTemplateInfo)this.getObject(EntityType.PROPERTYTEMPLATE, "Global", name);
    }
    
    public void remoteCallOnObject(final ActionType what, final MetaInfo.MetaObject obj, final Object[] params, final UUID clientSessionID) throws Exception {
        if (this.qv == null) {
            (this.qv = new QueryValidator(this)).setUpdateMode(true);
        }
        if (obj == null) {
            throw new RuntimeException(what + " on not existing object");
        }
        if (clientSessionID == null) {
            throw new RuntimeException("Authentication token not passed to load/unload function");
        }
        if (Server.logger.isInfoEnabled()) {
            Server.logger.info(String.format(Constant.REMOTE_CALL_MSG, what.toString(), obj.getFullName(), (params == null) ? 0 : params.length, clientSessionID.toString()));
        }
        MetaInfo.Flow autoBuiltFlow = null;
        final FlowComponent flowComponent = this.getOpenObject(obj.getUuid());
        Flow flow = null;
        if (flowComponent != null) {
            flow = flowComponent.getFlow();
        }
        try {
            assert obj != null;
            switch (what) {
                case LOAD: {
                    if (flow != null) {
                        throw new RuntimeException(String.format(Constant.ALREAD_LOADED_MSG, what.toString(), obj.getFullName()));
                    }
                    final String appname = obj.name + "_app";
                    if (null != this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, obj.nsName, appname, null, this.sessionID)) {
                        throw new RuntimeException("Cannot LOAD " + obj.name + " because application already exists: " + appname);
                    }
                    autoBuiltFlow = new ImplicitApplicationBuilder().init((MetaInfo.Namespace)this.getMetaObject(obj.getNamespaceId()), appname, this.sessionID).addObject(obj).build();
                    assert autoBuiltFlow != null;
                    if (autoBuiltFlow == null) {
                        throw new RuntimeException("Missing " + DeploymentRule.class.getSimpleName() + " as parameter");
                    }
                    final DeploymentRule dr = (DeploymentRule)params[0];
                    assert dr != null;
                    if (dr == null) {
                        throw new RuntimeException("Missing " + DeploymentRule.class.getSimpleName() + " as parameter");
                    }
                    dr.flowName = autoBuiltFlow.getFullName();
                    if (Server.logger.isInfoEnabled()) {
                        Server.logger.info(String.format(Constant.DEPLOY_START_MSG, dr, dr.flowName));
                    }
                    this.qv.LoadDataComponent(new AuthToken(clientSessionID), autoBuiltFlow, dr);
                    break;
                }
                case UNLOAD: {
                    if (flow == null) {
                        throw new RuntimeException(String.format(Constant.ALREAD_UNLOADED_MSG, what.toString(), obj.getFullName()));
                    }
                    this.qv.UnloadDataComponent(new AuthToken(clientSessionID), flow, obj);
                    break;
                }
                default: {
                    throw new RuntimeException(String.format(Constant.OPERATION_NOT_SUPPORTED, (what == null) ? "N/A" : what.toString()));
                }
            }
        }
        catch (Throwable e) {
            if (what == ActionType.LOAD) {
                if (autoBuiltFlow != null && autoBuiltFlow.getUuid() != null) {
                    this.metadataRepository.removeMetaObjectByUUID(autoBuiltFlow.getUuid(), new AuthToken(clientSessionID));
                }
                else if (Server.logger.isInfoEnabled()) {
                    Server.logger.info(String.format(Constant.LOAD_FAILED_WITH_NO_IMPLICIT_APP, (obj == null) ? "N/A" : obj.getFullName()));
                }
            }
            throw e;
        }
    }
    
    public void changeFlowState(final ActionType what, final MetaInfo.Flow info, final List<UUID> actionParam, final UUID clientSessionID, final List<Property> params, final Long epochNumber) throws Exception {
        assert info != null;
        switch (what) {
            case START: {
                this.startFlow(info, params, actionParam, epochNumber);
                break;
            }
            case VERIFY_START: {
                this.verifyStartFlow(info, actionParam);
                break;
            }
            case START_SOURCES: {
                this.startSources(info, actionParam);
                break;
            }
            case STOP: {
                this.stopFlow(info, epochNumber);
                break;
            }
            case QUIESCE: {
                this.quiesceFlow(info, epochNumber);
                break;
            }
            case PAUSE_SOURCES: {
                this.pauseSources(info, epochNumber);
                break;
            }
            case APPROVE_QUIESCE: {
                this.approveQuiesce(info, epochNumber);
                break;
            }
            case UNPAUSE_SOURCES: {
                this.unpauseSources(info, epochNumber);
                break;
            }
            case QUIESCE_FLUSH: {
                this.quiesceFlush(info, params, epochNumber);
                break;
            }
            case QUIESCE_CHECKPOINT: {
                this.quiesceCheckpoint(info, params, epochNumber);
                break;
            }
            case CHECKPOINT: {
                this.checkpoint(info, params, epochNumber);
                break;
            }
            case DEPLOY: {
                this.deployFlow(info, actionParam, true);
                break;
            }
            case INIT_CACHES: {
                this.initCaches(info);
                break;
            }
            case START_CACHES: {
                this.startCaches(info, actionParam, epochNumber);
                break;
            }
            case STOP_CACHES: {
                this.stopCaches(info, epochNumber);
                break;
            }
            case UNDEPLOY: {
                this.undeployFlow(info, clientSessionID);
                break;
            }
            case STATUS: {
                break;
            }
            case START_ADHOC: {
                this.deployStartQuery(info, clientSessionID, what, params);
                break;
            }
            case STOP_ADHOC: {
                this.stopUndeployQuery(info, clientSessionID, what);
                break;
            }
            default: {
                assert false;
                break;
            }
        }
    }
    
    public void checkNotDeployed(final MetaInfo.MetaObject obj) throws ServerException {
        final FlowComponent o = this.getOpenObject(obj.uuid);
        if (o != null) {
            throw new ServerException(obj.type + " already deployed on this server  " + obj.name + ":" + obj.type + ":" + obj.uuid);
        }
    }
    
    private String[] getAdHocQueryResultSetDesc(final MetaInfo.Flow info) throws ServerException, MetaDataRepositoryException {
        for (final UUID cqid : info.getObjects(EntityType.CQ)) {
            final MetaInfo.CQ cq = this.getObjectInfo(cqid, EntityType.CQ);
            if (cq.stream != null) {
                final List<RSFieldDesc> desc = cq.plan.resultSetDesc;
                final String[] fieldsInfo = new String[desc.size()];
                int i = 0;
                for (final RSFieldDesc f : desc) {
                    fieldsInfo[i] = f.name;
                    ++i;
                }
                return fieldsInfo;
            }
        }
        throw new RuntimeException("query has no result set description");
    }
    
    public Stream getStreamRuntime(final UUID uuid) throws MetaDataRepositoryException {
        Flow flowRuntime = null;
        for (final FlowComponent fc : this.openObjects.values()) {
            if (fc instanceof Flow && ((Flow)fc).getFlowMeta().getAllObjects().contains(uuid)) {
                flowRuntime = (Flow)fc;
                break;
            }
        }
        if (flowRuntime != null) {
            return flowRuntime.getUsedStreams().get(uuid);
        }
        return null;
    }
    
    public void deployStartQuery(final MetaInfo.Flow info, final UUID clientSessionID, final ActionType what, final List<Property> params) throws Exception {
        final LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(clientSessionID.toString(), info.getUuid().toString());
        final Lock lock = LocalLockProvider.getLock(keyForLock);
        lock.lock();
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Thread.currentThread().getName() + " : " + what + " : " + info.getFullName()));
        }
        try {
            final Flow f = this.deployAdhocFlow(info);
            if (f == null) {
                return;
            }
            f.bindParams(params);
            final String[] fieldsInfo = this.getAdHocQueryResultSetDesc(info);
            final UUID id = info.getObjects(EntityType.STREAM).iterator().next();
            final Stream outputStream = this.getStreamRuntime(id);
            if (outputStream == null) {
                throw new Exception("Could not find Stream runtime object with UUID - " + id + " on " + this.getServerID());
            }
            final String queueName = "consoleQueue" + clientSessionID.getUUIDString();
            final HQueue consoleQueue = HQueue.getQueue(queueName);
            final UUID queryMetaObjectUUID = info.getReverseIndexObjectDependencies().iterator().next();
            if (queryMetaObjectUUID == null) {
                return;
            }
            final AdhocStreamSubscriber adhocStreamSubscriber = new AdhocStreamSubscriber(fieldsInfo, queryMetaObjectUUID, queueName, consoleQueue);
            this.subscribe(outputStream, adhocStreamSubscriber);
            if (this.userQueryRecords.containsKey(clientSessionID)) {
                this.userQueryRecords.get(clientSessionID).put(info.uuid, Pair.make(outputStream, adhocStreamSubscriber));
            }
            else {
                final HashMap<UUID, Pair<Publisher, Subscriber>> newUserRecord = new HashMap<UUID, Pair<Publisher, Subscriber>>();
                newUserRecord.put(info.uuid, Pair.make(outputStream, adhocStreamSubscriber));
                this.userQueryRecords.put(clientSessionID, newUserRecord);
            }
            final List<UUID> servers = new ArrayList<UUID>();
            servers.add(getServer().getServerID());
            this.startFlow(info, null, servers, null);
            this.startSources(info, null);
        }
        finally {
            lock.unlock();
        }
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Thread.currentThread().getName() + " : " + what + " : " + info.getFullName() + " DONE."));
        }
    }
    
    public void stopUndeployQuery(final MetaInfo.Flow info, final UUID clientSessionID, final ActionType what) throws Exception {
        final LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(clientSessionID.toString(), info.getUuid().toString());
        final Lock lock = LocalLockProvider.getLock(keyForLock);
        lock.lock();
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Thread.currentThread().getName() + " : " + what + " : " + info.getFullName()));
        }
        try {
            if (this.getOpenObject(info.getUuid()) == null) {
                if (Server.logger.isDebugEnabled()) {
                    Server.logger.debug(("No such adhoc " + info.getFullName() + " is deployed in server " + this.getDebugId()));
                }
                return;
            }
            if (!this.userQueryRecords.containsKey(clientSessionID)) {
                return;
            }
            final Pair<Publisher, Subscriber> pubsub = this.userQueryRecords.get(clientSessionID).remove(info.uuid);
            if (this.userQueryRecords.get(clientSessionID).size() == 0) {
                this.userQueryRecords.remove(clientSessionID);
            }
            this.stopFlow(info, null);
            try {
                this.unsubscribe(pubsub.first, pubsub.second);
            }
            catch (Exception e) {
                Server.logger.warn(e.getMessage());
            }
            this.undeployFlow(info, null);
            if (Server.logger.isDebugEnabled()) {
                Server.logger.debug((Thread.currentThread().getName() + " : undeploying app : " + info.getFullName() + " done."));
            }
            if (HQueue.getQueue("consoleQueue" + clientSessionID.getUUIDString()).listeners.size() == 0) {
                HQueue.getQueue("consoleQueue" + clientSessionID.getUUIDString()).isActive = false;
                if (Server.logger.isDebugEnabled()) {
                    Server.logger.debug(("consoleQueue" + clientSessionID.getUUIDString() + " removed, due to no listeners"));
                }
                HQueue.removeQueue("consoleQueue" + clientSessionID.getUUIDString());
            }
        }
        finally {
            lock.unlock();
            LocalLockProvider.removeLock(keyForLock);
        }
        if (Server.logger.isDebugEnabled()) {
            Server.logger.debug((Thread.currentThread().getName() + " : " + what + " : " + info.getFullName() + " DONE."));
        }
    }
    
    public Flow deployFlow(final MetaInfo.Flow info, final List<UUID> actionParam, final boolean doStartNodeManager) throws Exception {
        if (actionParam == null) {
            return null;
        }
        Flow f = (Flow)this.getOpenObject(info.getUuid());
        if (f == null) {
            f = this.createFlow(info, null);
        }
        if (f == null) {
            Server.logger.warn(("Flow " + info.uuid + " cannot be deployed because create flow failed"));
            return null;
        }
        final boolean startNodeManager = true;
        try {
            f.deploySubFlows(actionParam);
            f.deploy(actionParam);
            if (this.countCaches(info) > 0) {
                this.initCaches(info);
            }
            if (startNodeManager && doStartNodeManager) {
                f.startNodeManager();
            }
        }
        catch (Throwable e) {
            Server.logger.error(("Deploy for flow: " + info.name + "(uuid: " + info.uuid + ") failed with exception " + e.getMessage()), e);
            this.removeDeployedObject(f);
            throw e;
        }
        List<MetaInfo.MetaObjectInfo> deployed = null;
        if (this.getOpenObject(info.uuid) != null) {
            deployed = Flow.getObjectsMeta(f.getDeployedObjects());
        }
        if (deployed != null) {
            this.ServerInfo.addCurrentObjectsInServer(f.getMetaName(), deployed);
            this.metadataRepository.putServer(this.ServerInfo, this.sessionID);
        }
        return f;
    }
    
    public Flow deployAdhocFlow(final MetaInfo.Flow info) throws Exception {
        if (DeployUtility.canAnyFlowOrSubFlowBeDeployed(info, this.getServerID()).isEmpty()) {
            return null;
        }
        this.checkNotDeployed(info);
        final Flow f = this.createFlow(info, null);
        if (f == null) {
            Server.logger.warn(("Flow " + info.uuid + " cannot be deployed because create flow failed"));
            return null;
        }
        final boolean startNodeManager = true;
        try {
            f.deployAdhocSubFlows();
            f.deployAdhocFlow();
            if (this.countCaches(info) > 0) {
                this.initCaches(info);
            }
        }
        catch (Throwable e) {
            Server.logger.error(("Deploy for flow: " + info.name + "(uuid: " + info.uuid + ") failed with exception " + e.getMessage()), e);
            this.removeDeployedObject(f);
            throw e;
        }
        List<MetaInfo.MetaObjectInfo> deployed = null;
        if (this.getOpenObject(info.uuid) != null) {
            deployed = Flow.getObjectsMeta(f.getDeployedObjects());
        }
        if (deployed != null) {
            this.ServerInfo.addCurrentObjectsInServer(f.getMetaName(), deployed);
            this.metadataRepository.putServer(this.ServerInfo, this.sessionID);
        }
        return f;
    }
    
    public void initCaches(final MetaInfo.Flow info) throws Exception {
        final Flow f = (Flow)this.getOpenObject(info.uuid);
        if (f != null) {
            f.initCaches();
        }
    }
    
    public void startCaches(final MetaInfo.Flow info, final List<UUID> servers, final long epochNumber) throws Exception {
        if (servers == null || servers.isEmpty()) {
            throw new Exception("Passed in list of servers is empty");
        }
        final Flow f = (Flow)this.getOpenObject(info.uuid);
        if (f != null) {
            f.startCaches(servers, epochNumber);
        }
    }
    
    public void stopCaches(final MetaInfo.Flow info, final long epochNumber) throws Exception {
        final Flow f = (Flow)this.getOpenObject(info.uuid);
        if (f != null) {
            f.stopCaches(epochNumber);
        }
    }
    
    public void startFlow(final MetaInfo.Flow info, final List<Property> params, final List<UUID> servers, final Long epochNumber) throws Exception {
        final Flow flow = this.setupRecovery(info, params);
        if (servers != null) {
            flow.startFlow(servers, epochNumber);
        }
        else {
            flow.startFlow(epochNumber);
        }
    }
    
    public Flow setupRecovery(final MetaInfo.Flow info, final List<Property> params) throws Exception {
        assert info != null;
        final Flow flow = (Flow)this.getOpenObject(info.uuid);
        if (flow == null) {
            Server.logger.warn(("Cannot find application to start: " + info.name));
            return flow;
        }
        ILock appCheckpointLock = null;
        ILock wsCheckpointLock = null;
        RecoveryDescription recov = null;
        String signalsSetName = "default";
        if (params != null) {
            for (final Property param : params) {
                if (param.name.equals("RecoveryDescription")) {
                    recov = (RecoveryDescription)param.value;
                }
                if (param.name.equals("StartupSignalsSetName")) {
                    signalsSetName = (String)param.value;
                }
            }
        }
        if (recov != null) {
            if (recov.type == 3) {
                final ISet signalsSet = HazelcastSingleton.get().getSet(signalsSetName);
                try {
                    appCheckpointLock = HazelcastSingleton.get().getLock(info.uuid.toString());
                    appCheckpointLock.lock();
                    if (!signalsSet.contains(info.uuid.toString())) {
                        StatusDataStore.getInstance().clearAppCheckpoint(flow.getMetaID());
                        signalsSet.add(info.uuid.toString());
                    }
                }
                finally {
                    if (appCheckpointLock != null) {
                        appCheckpointLock.unlock();
                    }
                }
                final List<HStore> allHDStores = flow.getAllHDStores();
                final Stack<HStore> failHDStores = new Stack<HStore>();
                for (final HStore ws : allHDStores) {
                    try {
                        wsCheckpointLock = HazelcastSingleton.get().getLock(ws.getMetaID().toString());
                        wsCheckpointLock.lock();
                        if (signalsSet.contains(ws.getMetaID().toString())) {
                            continue;
                        }
                        if (Logger.getLogger("Recovery").isDebugEnabled()) {
                            Logger.getLogger("Recovery").debug(("Resetting the HDStore Checkpoint for WS " + ws.getMetaName() + " on node " + BaseServer.getServerName()));
                        }
                        if (!ws.clearHDStoreCheckpoint()) {
                            failHDStores.push(ws);
                        }
                        signalsSet.add(ws.getMetaID().toString());
                    }
                    finally {
                        if (wsCheckpointLock != null) {
                            wsCheckpointLock.unlock();
                        }
                    }
                }
                if (!failHDStores.isEmpty()) {
                    final StringBuilder names = new StringBuilder("Failed to clear HDStore checkpoint for: ");
                    for (final HStore ws2 : failHDStores) {
                        names.append(ws2.getMetaName()).append(" ");
                    }
                    throw new Exception(names.toString());
                }
            }
            flow.setRecoveryType(2);
            flow.setRecoveryPeriod(recov.interval);
        }
        else {
            flow.setRecoveryType(flow.flowInfo.recoveryType);
            flow.setRecoveryPeriod(flow.flowInfo.recoveryPeriod);
        }
        return flow;
    }
    
    public void verifyStartFlow(final MetaInfo.Flow info, final List<UUID> servers) throws Exception {
        if (servers == null || servers.isEmpty()) {
            throw new Exception("Passed in list of servers is empty");
        }
        if (servers != null) {
            final Flow fl = (Flow)this.getOpenObject(info.uuid);
            if (fl == null) {
                throw new Exception("Flow is not deployed on this node. Failed to verify flow start");
            }
            if (!fl.verifyAppStart(servers)) {
                throw new Exception("Start flow failed this time. Failed to verify flow start");
            }
        }
    }
    
    public void startSources(final MetaInfo.Flow info, final List<UUID> servers) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            try {
                fl.startSources(servers);
            }
            catch (Exception e) {
                Server.logger.warn("Failure in Starting Sources.", (Throwable)e);
                throw e;
            }
        }
    }
    
    public void stopFlow(final MetaInfo.Flow info, final Long epochNumber) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.stopFlow(epochNumber);
        }
    }
    
    public void pauseSources(final MetaInfo.Flow info, final Long epochNumber) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.pauseSources(epochNumber);
        }
    }
    
    public void quiesceFlow(final MetaInfo.Flow info, final Long epochNumber) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.quiesceFlow(epochNumber);
        }
    }
    
    public void approveQuiesce(final MetaInfo.Flow info, final Long epochNumber) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.approveQuiesce(epochNumber);
        }
    }
    
    public void unpauseSources(final MetaInfo.Flow info, final Long epochNumber) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.unpauseSources(epochNumber);
        }
    }
    
    public void checkpoint(final MetaInfo.Flow info, final List<Property> params, final Long epochNumber) throws Exception {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            final Logger logger = Server.logger;
            Logger.getLogger("Recovery").debug(("Node has been instructed to checkpoint flow " + info.name));
        }
        assert info != null;
        for (final Property p : params) {
            if (p.name.equals("COMMAND_TIMESTAMP")) {
                final Long commandTimestamp = (Long)p.value;
                final Flow fl = (Flow)this.getOpenObject(info.uuid);
                if (fl == null) {
                    EventQueueManager.get().sendCommandConfirmationEvent(this.getServerID(), Event.EventAction.NODE_APP_CHECKPOINTED, info.uuid, commandTimestamp);
                    return;
                }
                fl.checkpoint(commandTimestamp);
                return;
            }
        }
        Server.logger.warn(("Could not checkpoint application " + info.name + " because it came with no timestamp!"));
    }
    
    public void quiesceCheckpoint(final MetaInfo.Flow info, final List<Property> params, final Long epochNumber) throws Exception {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            final Logger logger = Server.logger;
            Logger.getLogger("Recovery").debug(("Node has been instructed to quiesce-checkpoint flow " + info.name));
        }
        assert info != null;
        for (final Property p : params) {
            if (p.name.equals("COMMAND_TIMESTAMP")) {
                final Long commandTimestamp = (Long)p.value;
                final Flow fl = (Flow)this.getOpenObject(info.uuid);
                if (fl == null) {
                    EventQueueManager.get().sendCommandConfirmationEvent(this.getServerID(), Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED, info.uuid, commandTimestamp);
                    return;
                }
                fl.quiesceCheckpoint(commandTimestamp);
                return;
            }
        }
        Server.logger.warn(("Could not checkpoint application " + info.name + " because it came with no timestamp!"));
    }
    
    public void quiesceFlush(final MetaInfo.Flow info, final List<Property> params, final Long epochNumber) throws Exception {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            final Logger logger = Server.logger;
            Logger.getLogger("Recovery").debug(("Node has been instructed to quiesce-flush flow " + info.name));
        }
        assert info != null;
        for (final Property p : params) {
            if (p.name.equals("COMMAND_TIMESTAMP")) {
                final Long commandTimestamp = (Long)p.value;
                final Flow fl = (Flow)this.getOpenObject(info.uuid);
                if (fl == null) {
                    EventQueueManager.get().sendCommandConfirmationEvent(this.getServerID(), Event.EventAction.NODE_APP_QUIESCE_FLUSHED, info.uuid, commandTimestamp);
                    return;
                }
                fl.quiesceFlush(commandTimestamp);
            }
        }
    }
    
    public void memoryStatus(final MetaInfo.Flow info) throws Exception {
        assert info != null;
        final Flow fl = (Flow)this.getOpenObject(info.uuid);
        if (fl != null) {
            fl.memoryStatus();
        }
    }
    
    private void closeNotUsedStreams(final List<Flow> flows) throws Exception {
        for (final Flow f : flows) {
            final Map<UUID, Stream> used_streams = f.getUsedStreams();
            for (final Stream stream_runtime : used_streams.values()) {
                if (stream_runtime != null) {
                    for (final Flow flow : flows) {
                        stream_runtime.disconnectFlow(flow);
                    }
                    if (stream_runtime.isConnected() || stream_runtime.getMetaFullName().equalsIgnoreCase("Global.exceptionsStream")) {
                        continue;
                    }
                    this.removeDeployedObject(stream_runtime);
                }
            }
        }
    }
    
    public void undeployFlow(final MetaInfo.Flow info, final UUID clientSessionID) throws Exception {
        assert info != null;
        this.verifyQueries(info, clientSessionID);
        final Flow f = (Flow)this.getOpenObject(info.uuid);
        if (f != null) {
            final List<Flow> flows = f.getAllFlows();
            final List<MetaInfo.MetaObjectInfo> undeployed = Flow.getObjectsMeta(f.getDeployedObjects());
            this.removeDeployedObject(f);
            this.closeNotUsedStreams(flows);
            this.ServerInfo.removeCurrentObjectsInServer(f.getMetaName(), undeployed);
            this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
        }
    }
    
    private Set<UUID> findDataSourcesUUID(final MetaInfo.Flow application) throws MetaDataRepositoryException {
        final EntityType[] dataSources = { EntityType.STREAM, EntityType.CACHE, EntityType.HDSTORE };
        final Set<UUID> objectsBelongToApplication = new LinkedHashSet<UUID>();
        for (final EntityType entityType : dataSources) {
            objectsBelongToApplication.addAll(application.getObjects(entityType));
        }
        final Set<UUID> subFlows = application.getObjects(EntityType.FLOW);
        if (subFlows != null) {
            for (final UUID subFlowUUID : subFlows) {
                for (final EntityType entityType2 : dataSources) {
                    final MetaInfo.Flow subFlow = (MetaInfo.Flow)this.getMetaObject(subFlowUUID);
                    objectsBelongToApplication.addAll(subFlow.getObjects(entityType2));
                }
            }
        }
        return objectsBelongToApplication;
    }
    
    private Set<MetaInfo.MetaObject> findDataSources(final Set<UUID> allQueriableDataSources) throws MetaDataRepositoryException {
        final Set<MetaInfo.MetaObject> objectsBelongToApplication = new LinkedHashSet<MetaInfo.MetaObject>();
        for (final UUID uuid : allQueriableDataSources) {
            final MetaInfo.MetaObject metaObject = this.getObject(uuid);
            if (metaObject != null) {
                objectsBelongToApplication.add(metaObject);
            }
        }
        return objectsBelongToApplication;
    }
    
    private void verifyQueries(final MetaInfo.Flow info, final UUID clientUUID) throws MetaDataRepositoryException {
        if (clientUUID == null) {
            return;
        }
        final Set<MetaInfo.MetaObject> allQueriableDataSources = this.findDataSources(this.findDataSourcesUUID(info));
        final Set<UUID> reverseObjectsFromQueriableDataSources = new LinkedHashSet<UUID>();
        for (final MetaInfo.MetaObject queriableObject : allQueriableDataSources) {
            reverseObjectsFromQueriableDataSources.addAll(queriableObject.getReverseIndexObjectDependencies());
        }
        final Set<MetaInfo.MetaObject> adhocQueryRelatedObjects = new LinkedHashSet<MetaInfo.MetaObject>();
        for (final UUID reverseIndex : reverseObjectsFromQueriableDataSources) {
            final MetaInfo.MetaObject reverseIndexedMetaObject = this.getObject(reverseIndex);
            if (reverseIndexedMetaObject != null && reverseIndexedMetaObject.getMetaInfoStatus().isAdhoc()) {
                adhocQueryRelatedObjects.add(reverseIndexedMetaObject);
            }
        }
        final Set<MetaInfo.Flow> generatedAdhocApplications = new LinkedHashSet<MetaInfo.Flow>();
        for (final MetaInfo.MetaObject metaObject : adhocQueryRelatedObjects) {
            final MetaInfo.Flow application = metaObject.getCurrentApp();
            if (application != null && application.getMetaInfoStatus().isAdhoc()) {
                generatedAdhocApplications.add(application);
            }
        }
        for (final MetaInfo.Flow application2 : generatedAdhocApplications) {
            if (application2 != null && application2.getReverseIndexObjectDependencies() != null) {
                final Iterator<UUID> iterator = application2.getReverseIndexObjectDependencies().iterator();
                if (!iterator.hasNext()) {
                    continue;
                }
                final UUID queryUUID = application2.getReverseIndexObjectDependencies().iterator().next();
                if (queryUUID == null) {
                    continue;
                }
                final QueryValidator queryValidator = new QueryValidator(this);
                queryValidator.createNewContext(new AuthToken(clientUUID));
                queryValidator.stopAdhocQuery(new AuthToken(clientUUID), queryUUID);
            }
        }
    }
    
    @Override
    public void showStreamStmt(final MetaInfo.ShowStream show_stream) throws Exception {
        final Stream stream = show_stream.getLocalStreamRuntime();
        if (stream != null) {
            stream.initQueue(show_stream);
        }
    }
    
    public static void shutDown(final String who) throws Exception {
        if (Server.logger.isInfoEnabled()) {
            Server.logger.info(("shutdown " + who));
        }
    }
    
    private static WebServer startWebServer(final Server srv) {
        final Lock webServerLock = (Lock)HazelcastSingleton.get().getLock("startingWebServer");
        webServerLock.lock();
        try {
            final QueryValidator qv = new QueryValidator(srv);
            final Context ctx = srv.createContext();
            qv.addContext(ctx.getAuthToken(), ctx);
            final MetadataHelper metadataHelper = new MetadataHelper(qv);
            final DashboardAuthenticator dashboardAuthenticator = new DashboardAuthenticator(qv);
            final ApplicationHelper appHelper = new ApplicationHelper();
            final FileMetadataAPIHelper fileMetadataAPIHelper = new FileMetadataAPIHelper();
            Server.webServer = new WebServer(HazelcastSingleton.getBindingInterface());
            try {
                final Class<?> oracleWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.source.oraclecommon.OracleCDCWizard");
                final Class<?> mssqlWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.source.mssql.MSSqlWizard");
                final Class<?> mysqlWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.source.mysql.MySQLWizard");
                final Class<?> hazelcastWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.hazelcast.wizard.HazelcastWizard");
                final Class<?> bqWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.BigQueryWriterWizard");
                final Class<?> dbwriterWizClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.wizard.DatabaseWriterWizard");
                final Class<?> hdfsWriterWizardClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.HDFSWriter_1_0");
                final Class<?> azureBlobWriterWizardClass = ClassLoader.getSystemClassLoader().loadClass("com.striim.ui.wizard.AzureBlobWriterWizard");
                final Class<?> KafkaWriter8WizardClass = ClassLoader.getSystemClassLoader().loadClass("com.striim.ui.wizard.KafkaWriter8Wizard");
                final Class<?> kafkaWriter9WizardClass = ClassLoader.getSystemClassLoader().loadClass("com.striim.ui.wizard.KafkaWriter9Wizard");
                final Class<?> kafkaWriter10WizardClass = ClassLoader.getSystemClassLoader().loadClass("com.striim.ui.wizard.KafkaWriter10Wizard");
                final Class<?> hdInsightHdfsWriterClass = ClassLoader.getSystemClassLoader().loadClass("com.striim.proc.HDInsightHDFSWriter");
                final ICDCWizard msWiz = (ICDCWizard)mssqlWizClass.newInstance();
                final ICDCWizard OraWiz = (ICDCWizard)oracleWizClass.newInstance();
                final ICDCWizard mysqlWiz = (ICDCWizard)mysqlWizClass.newInstance();
                final IHazelcastWizard hazlecastWiz = (IHazelcastWizard)hazelcastWizClass.newInstance();
                final TargetWizard bqWiz = (TargetWizard)bqWizClass.newInstance();
                final TargetWizard dbwriterWiz = (TargetWizard)dbwriterWizClass.newInstance();
                RMIWebSocket.register(HDApi.get());
                RMIWebSocket.register(HSecurityManager.get());
                RMIWebSocket.register(MetadataRepository.getINSTANCE());
                RMIWebSocket.register(qv);
                RMIWebSocket.register(metadataHelper);
                RMIWebSocket.register(dashboardAuthenticator);
                RMIWebSocket.register(appHelper);
                RMIWebSocket.register(fileMetadataAPIHelper);
                RMIWebSocket.register(Usage.getInstance());
                RMIWebSocket.register(OraWiz);
                RMIWebSocket.register(msWiz);
                RMIWebSocket.register(mysqlWiz);
                RMIWebSocket.register(hazlecastWiz);
                RMIWebSocket.register(bqWiz);
                RMIWebSocket.register(dbwriterWiz);
                RMIWebSocket.register(hdfsWriterWizardClass.newInstance());
                RMIWebSocket.register(azureBlobWriterWizardClass.newInstance());
                RMIWebSocket.register(KafkaWriter8WizardClass.newInstance());
                RMIWebSocket.register(kafkaWriter9WizardClass.newInstance());
                RMIWebSocket.register(kafkaWriter10WizardClass.newInstance());
                RMIWebSocket.register(hdInsightHdfsWriterClass.newInstance());
            }
            catch (Exception e) {
                Server.logger.error("Problem registering web server WS classes", (Throwable)e);
            }
            try {
                Server.webServer.setRestServletContextHandler();
                Server.webServer.addContextHandler("/rmiws", (Handler)new RMIWebSocket.Handler());
                Server.webServer.addServletContextHandler("/health/*", new HealthServlet());
                Server.webServer.addResourceHandler("/web", "./Platform/web", true, new String[] { "index.html" });
                Server.webServer.addResourceHandler("/login", "./ui", true, new String[] { "login/index.html" });
                Server.webServer.addResourceHandler("/dashboards", "./ui", true, new String[] { "dashboards/index.html" });
                Server.webServer.addResourceHandler("/dashboard", "./ui", true, new String[] { "dashboard_index.html" });
                Server.webServer.addResourceHandler("/dashboard/edit", "./ui", true, new String[] { "edit-visualizations.html" });
                Server.webServer.addResourceHandler("/monitor", "./ui", true, new String[] { "monitor_index.html" });
                Server.webServer.addResourceHandler("/admin", "./ui/admin", true, new String[] { "index.html" });
                Server.webServer.addResourceHandler("/docs", "./webui/onlinedocs/", true, new String[] { "Striim_documentation.html" });
                Server.webServer.addResourceHandler("/flow", "./ui", true, new String[] { "flow/flow.html" });
                Server.webServer.addResourceHandler("/flow/edit", "./ui", true, new String[] { "flow/flow.html" });
                Server.webServer.addResourceHandler("/root", "./ui", false, null);
                Server.webServer.addResourceHandler("/webui", "./webui", true, new String[] { "index.html" });
                Server.webServer.addResourceHandler("/", "./ui", true, new String[] { "index.html" });
                Server.webServer.addServletContextHandler("/metadata/*", new MetadataRepositoryServlet());
                Server.webServer.addServletContextHandler("/api/metadata/*", new MetadataRepositoryServlet());
                Server.webServer.addServletContextHandler("/hds/*", new HDApiServlet());
                Server.webServer.addServletContextHandler("/security/*", new SecurityManagerServlet());
                Server.webServer.addServletContextHandler("/upload/*", new FileUploaderServlet());
                Server.webServer.addServletContextHandler("/file_browser/*", new FileSystemBrowserServlet());
                Server.webServer.addServletContextHandler("/preview/*", new DataPreviewServlet());
                Server.webServer.addServletContextHandler("/streams/*", new StreamInfoServlet());
                RMIWebSocket.initCleanupRoutine();
            }
            catch (Exception e) {
                Server.logger.error("Problem creating web server contexts", (Throwable)e);
            }
            try {
                Server.webServer.start();
            }
            catch (Exception e) {
                Server.logger.error("Problem starting web server", (Throwable)e);
                throw new RuntimeException("Problem starting web server: " + e.getMessage());
            }
        }
        finally {
            webServerLock.unlock();
        }
        return Server.webServer;
    }
    
    public void startDiscoveryServer() {
        final UdpDiscoveryServer discoveryServer = new UdpDiscoveryServer(Server.webServer.getHttpPort(), HSecurityManager.ENCRYPTION_SALT);
        this.discoveryTask = this.pool.submit(discoveryServer);
    }
    
    public void updateWebUri() {
        final String baseHttpUri = "http://" + HazelcastSingleton.getBindingInterface() + ":" + Server.webServer.getHttpPort();
        try {
            this.ServerInfo.setWebBaseUri(baseHttpUri);
            this.metadataRepository.putServer(this.ServerInfo, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            Server.logger.warn(("Failed to add base URI to ServerInfo " + baseHttpUri), (Throwable)e);
        }
    }
    
    public int printOpenObjs() {
        Server.logger.info("Open Objs");
        if (this.openObjects != null) {
            Server.logger.info(("Size: " + this.openObjects.size()));
            return this.openObjects.size();
        }
        return 0;
    }
    
    public void initializeMDRepoCRUDWebNotificationHandler() {
        final MDRepositoryCRUDWebNotificationHandler handler = new MDRepositoryCRUDWebNotificationHandler();
        this.metadataRepository.registerListenerForMetaObject(handler);
    }
    
    public void initializeStatusInfoWebNotificationHandler() {
        final MetaObjectStatusChangeWebNotificationHandler handler = new MetaObjectStatusChangeWebNotificationHandler();
        this.metadataRepository.registerListenerForStatusInfo(handler);
    }
    
    public static void main(final String[] args) throws Exception {
        printf("Starting Striim Server - " + Version.getVersionString() + "\n");
        if (Server.logger.isInfoEnabled()) {
            Server.logger.info(("Starting Striim Server - " + Version.getVersionString() + "\n"));
        }
        Server.server = new Server();
        final ILock lock = HazelcastSingleton.get().getLock("#ServerStartupLock");
        lock.lock();
        try {
            Server.server.initializeHazelcast();
            HDStores.startup();
            Server.server.checkVersion();
            Server.server.setSecMgrAndOther();
            Server.server.addAlertQueue("systemAlerts");
            Server.server.initExceptionHandler();
            Server.server.initShowStream();
            Server.server.startAppManager();
            WebServer webServer = null;
            try {
                webServer = startWebServer(Server.server);
                Server.server.updateWebUri();
                Server.server.startDiscoveryServer();
            }
            catch (Exception e) {
                Server.logger.error((e.getMessage() + " - Shutting down!"));
                shutDown("Web Server Failure");
                System.exit(77);
            }
            Server.server.initializeMDRepoCRUDWebNotificationHandler();
            Server.server.initializeStatusInfoWebNotificationHandler();
            HSecurityManager.setFirstTime(false);
            if (System.getProperty("com.datasphere.config.odbcenabled", "false").equalsIgnoreCase("true")) {
                try {
                    System.loadLibrary("HDODBCJNI");
                    Server.server.setClassLoader(WALoader.get());
                    Server.server.initSimbaServer(HazelcastSingleton.getBindingInterface());
                }
                catch (UnsatisfiedLinkError e2) {
                    Server.logger.error(("Unable to initialize ODBC driver. Reason - " + e2.getLocalizedMessage()));
                }
            }
            printf("\nstarted.");
            final String srvInterface = HazelcastSingleton.getBindingInterface();
            final int wsHttpPort = webServer.getHttpPort();
            final int wsHttpsPort = webServer.getHttpsPort();
            if (wsHttpPort != -1 && wsHttpsPort != -1) {
                printf("\nPlease go to http://" + ((srvInterface != null) ? srvInterface : "localhost") + ":" + wsHttpPort + " or https://" + ((srvInterface != null) ? srvInterface : "localhost") + ":" + wsHttpsPort + " to administer, or use console\n");
            }
            else if (wsHttpPort != -1) {
                printf("\nPlease go to http://" + ((srvInterface != null) ? srvInterface : "localhost") + ":" + wsHttpPort + " to administer, or use console\n");
            }
            else if (wsHttpsPort != -1) {
                printf("\nPlease go to https://" + ((srvInterface != null) ? srvInterface : "localhost") + ":" + wsHttpsPort + " to administer, or use console\n");
            }
            else {
                printf("\nNo WebServer configured, please use the console to administer\n");
            }
            if (System.getProperty("com.datasphere.integration.test.util.DistributedTestHandlerLoader", "false").equalsIgnoreCase("true")) {
                try {
                    final Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass("com.datasphere.integration.test.util.DistributedTestHandlerLoader");
                    if (clazz != null) {
                        final Object chorusHandlers = clazz.newInstance();
                        final Method loadH = clazz.getMethod("loadHandlers", (Class<?>[])new Class[0]);
                        if (loadH != null) {
                            loadH.invoke(chorusHandlers, new Object[0]);
                        }
                    }
                }
                catch (Exception ex) {}
            }
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Server.shutDown("server");
                    }
                    catch (Exception e) {
                        Server.logger.warn("Problem shutting down", (Throwable)e);
                    }
                }
            }));
        }
        finally {
            lock.unlock();
        }
    }
    
    @Override
    public Collection<FlowComponent> getAllObjectsView() {
        return this.openObjects.values();
    }
    
    @Override
    public Collection<Channel> getAllChannelsView() {
        return Collections.emptyList();
    }
    
    @Override
    public UUID getServerID() {
        return this.serverID;
    }
    
    private String s2n(final UUID sid) {
        return sid.toString();
    }
    
    private String getDebugId() {
        return this.s2n(this.serverID);
    }
    
    private static void addLibraryPath(final File file) {
        try {
            final Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(ClassLoader.getSystemClassLoader(), file.toURI().toURL());
        }
        catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | MalformedURLException ex2) {
        		ex2.printStackTrace();
        }
    }
    
    public Context createContext() {
        return new Context(this.sessionID);
    }
    
    public Context createContext(final AuthToken token) {
        return new Context(token);
    }
    
    public static double evictionThresholdPercent() {
        final double defaultVal = 66.0;
        if (HazelcastSingleton.isAvailable()) {
            if (HazelcastSingleton.isClientMember()) {
                return defaultVal;
            }
            final Object val = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.datasphere.config.hd-eviction-threshold");
            if (val != null) {
                return (double)val;
            }
        }
        final String localProp = System.getProperty("com.datasphere.config.hd-eviction-threshold");
        Double dVal;
        if (localProp != null) {
            try {
                dVal = Double.parseDouble(localProp);
            }
            catch (NumberFormatException e) {
                dVal = defaultVal;
            }
        }
        else {
            dVal = defaultVal;
        }
        if (HazelcastSingleton.isAvailable()) {
            HazelcastSingleton.get().getMap("#ClusterSettings").put("com.datasphere.config.hd-eviction-threshold", dVal);
        }
        return dVal;
    }
    
    public static boolean persistenceIsEnabled() {
        if (HazelcastSingleton.isAvailable()) {
            if (HazelcastSingleton.isClientMember()) {
                return false;
            }
            final Object enablePersist = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.datasphere.config.persist");
            if (enablePersist != null) {
                return enablePersist.toString().equalsIgnoreCase("TRUE");
            }
        }
        final String localProp = System.getProperty("com.datasphere.config.persist");
        final Boolean b = localProp == null || !localProp.equalsIgnoreCase("FALSE");
        if (HazelcastSingleton.isAvailable()) {
            HazelcastSingleton.get().getMap("#ClusterSettings").put("com.datasphere.config.persist", b);
        }
        return b;
    }
    
    private static void printf(final String toPrint) {
        if (System.console() != null) {
            System.console().printf(toPrint, new Object[0]);
        }
        else {
            System.out.printf(toPrint, new Object[0]);
        }
        System.out.flush();
    }
    
    public void entryRemoved(final EntryEvent<String, Object> event) {
    }
    
    public void entryEvicted(final EntryEvent<String, Object> event) {
    }
    
    public void entryAdded(final EntryEvent<String, Object> event) {
        this.entryUpdated(event);
    }
    
    public void entryUpdated(final EntryEvent<String, Object> event) {
        if (((String)event.getKey()).equalsIgnoreCase("com.datasphere.config.enable-monitor-persistence")) {
            final MonitorModel monitorModel = MonitorModel.getInstance();
            if (monitorModel != null && event.getValue() != null) {
                final boolean turnOn = event.getValue().toString().equalsIgnoreCase("TRUE");
                monitorModel.setPersistence(turnOn);
            }
        }
    }
    
    public MetaInfo.StatusInfo.Status getFlowStatus(MetaInfo.Flow flow) throws Exception {
        final Flow f = (Flow)this.getOpenObject(flow.uuid);
        if (f == null) {
            return MetaInfo.StatusInfo.Status.CREATED;
        }
        flow = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(flow.getUuid(), HSecurityManager.TOKEN);
        if (flow != null) {
            return flow.getFlowStatus();
        }
        return MetaInfo.StatusInfo.Status.UNKNOWN;
    }
    
    public Map<UUID, Boolean> whatIsRunning(final UUID flowId) throws Exception {
        final Map<UUID, Boolean> map = new HashMap<UUID, Boolean>();
        final Flow f = (Flow)this.getOpenObject(flowId);
        if (f != null) {
            for (final FlowComponent o : f.getDeployedObjects()) {
                if (o instanceof Restartable) {
                    final Restartable r = (Restartable)o;
                    final boolean running = r.isRunning();
                    map.put(o.getMetaID(), running);
                }
            }
        }
        return map;
    }
    
    public Collection<MetaInfo.MetaObjectInfo> getDeployedObjects(final UUID flowId) throws Exception {
        final Flow f = (Flow)this.getOpenObject(flowId);
        if (f != null) {
            return Flow.getObjectsMeta(f.getDeployedObjects());
        }
        return Collections.emptyList();
    }
    
    public void onItem(final Object item) {
        if (item != null && item instanceof ExceptionEvent) {
            final ExceptionEvent ee = (ExceptionEvent)item;
            if (ee.action.ordinal() == ActionType.STOP.ordinal()) {
                try {
                    final UUID id = ee.getAppid();
                    if (id != null) {
                        final MetaInfo.Flow flow = (MetaInfo.Flow)this.getObject(id);
                        this.stopFlow(flow, null);
                    }
                    else if (Server.logger.isInfoEnabled()) {
                        Server.logger.info("exception even sent without app uuid info");
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    @Override
    public boolean isServer() {
        return true;
    }
    
    public void mapEvicted(final MapEvent event) {
    }
    
    public void mapCleared(final MapEvent event) {
    }
    
    public void clientConnected(final Client arg0) {
    }
    
    public void clientDisconnected(final Client arg0) {
        final ILock lock = HazelcastSingleton.get().getLock("ServerList");
        lock.lock();
        try {
            final MetaInfo.Server removedAgent = (MetaInfo.Server)this.metadataRepository.getMetaObjectByUUID(new UUID(arg0.getUuid()), this.sessionID);
            if (removedAgent == null) {
                this.getShowStream().cleanChannelSubscriber(arg0.getUuid());
            }
            else if (removedAgent != null && removedAgent.isAgent) {
                final Collection<Client> members = (Collection<Client>)HazelcastSingleton.get().getClientService().getConnectedClients();
                final List<UUID> valid = new ArrayList<UUID>();
                for (final Client cl : members) {
                    valid.add(new UUID(cl.getUuid()));
                }
                final List<UUID> toRemove = new ArrayList<UUID>();
                final Set<MetaInfo.MetaObject> agents = (Set<MetaInfo.MetaObject>)this.metadataRepository.getByEntityType(EntityType.SERVER, this.sessionID);
                for (final MetaInfo.MetaObject obj : agents) {
                    final MetaInfo.Server agent = (MetaInfo.Server)obj;
                    if (!valid.contains(agent.uuid) && agent.isAgent) {
                        toRemove.add(agent.uuid);
                    }
                }
                for (final UUID uuid : toRemove) {
                    if (Server.logger.isDebugEnabled()) {
                        Server.logger.debug(("Trying to remove Agent : " + uuid + " from \n" + valid));
                    }
                    this.metadataRepository.removeMetaObjectByUUID(uuid, this.sessionID);
                }
                this.removeServerFromDeploymentGroup((Endpoint)arg0);
            }
            trimAuthTokens(arg0.getUuid());
        }
        catch (MetaDataRepositoryException e) {
            Server.logger.error(e);
        }
        catch (Throwable e2) {
            Server.logger.error(("Failed to remove member with exception " + e2));
        }
        finally {
            lock.unlock();
        }
        final UUID consoleQueueAuthToken = (UUID)Server.nodeIDToAuthToken.get(new UUID(arg0.getUuid()));
        if (consoleQueueAuthToken != null) {
            DistributedExecutionManager.exec(HazelcastSingleton.get(), (Callable<Object>)new AdhocCleanup(consoleQueueAuthToken.getUUIDString()), HazelcastSingleton.get().getCluster().getMembers());
        }
        final Collection<FlowComponent> flComps = Server.server.getAllObjectsView();
        for (final FlowComponent comp : flComps) {
            if (comp.getMetaType() == EntityType.APPLICATION && !comp.getMetaNsName().equals("Global")) {
                try {
                    final MetaInfo.StatusInfo statusInfo = MetadataRepository.getINSTANCE().getStatusInfo(comp.getMetaID(), HSecurityManager.TOKEN);
                    if (statusInfo.getStatus() != MetaInfo.StatusInfo.Status.RUNNING && statusInfo.getStatus() != MetaInfo.StatusInfo.Status.DEPLOYED) {
                        continue;
                    }
                    comp.removeSessionToReport((AuthToken)consoleQueueAuthToken, false);
                }
                catch (Exception e3) {}
            }
        }
    }
    
    public static void trimAuthTokens(final String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return;
        }
        try {
            final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
            boolean found = false;
            for (final AuthToken authToken : authTokens.keySet()) {
                final SessionInfo sessionInfo = (SessionInfo)authTokens.get((Object)authToken);
                if (sessionInfo != null && sessionInfo.clientId != null && sessionInfo.clientId.equals(clientId)) {
                    if (Server.logger.isInfoEnabled()) {
                        Server.logger.info((Object)("Removed " + sessionInfo.type + ": " + clientId));
                    }
                    authTokens.remove((Object)authToken);
                    found = true;
                    break;
                }
            }
            if (!found) {
                Server.logger.warn((Object)("Could not find session \"" + clientId + "\" for token removal"));
            }
        }
        catch (Exception e) {
            Server.logger.error((Object)("Error when trimming " + clientId + " from auth tokens"), (Throwable)e);
        }
    }
    
    public FlowComponent createComponent(final MetaInfo.MetaObject o) throws Exception {
        switch (o.getType()) {
            case CACHE: {
                return this.createCache((MetaInfo.Cache)o);
            }
            case CQ: {
                return this.createCQ((MetaInfo.CQ)o);
            }
            case SORTER: {
                return this.createSorter((MetaInfo.Sorter)o);
            }
            case SOURCE: {
                return this.createSource((MetaInfo.Source)o);
            }
            case STREAM_GENERATOR: {
                return this.createStreamGen((MetaInfo.StreamGenerator)o);
            }
            case TARGET: {
                return this.createTarget((MetaInfo.Target)o);
            }
            case HDSTORE: {
                return this.createHDStore((MetaInfo.HDStore)o);
            }
            case WASTOREVIEW: {
                return this.createWAStoreView((MetaInfo.WAStoreView)o);
            }
            case WINDOW: {
                return this.createWindow((MetaInfo.Window)o);
            }
            default: {
                return null;
            }
        }
    }
    
    public MultiComponent createMultiComponent(final MetaInfo.MetaObject o) throws MetaDataRepositoryException {
        final MultiComponent mc = new MultiComponent(this, o);
        this.putOpenObject(mc);
        return mc;
    }
    
    public void changeOptions(final String paramname, final Object paramvalue) {
        if (paramname.equalsIgnoreCase("loglevel")) {
            Utility.changeLogLevel(paramvalue, false);
        }
    }
    
    static {
        Server.logger = Logger.getLogger((Class)Server.class);
        azureSQLServerTemplates = new String[] { "OracleReader", "MysqlReader", "MSSqlReader", "FileReader", "HTTPReader", "TCPReader", "FreeFormTextParser", "AvroParser", "XMLParser", "JSONParser", "DSVParser", "DatabaseReader", "DatabaseWriter", "SysOut", "FileWriter", "XMLFormatter", "AvroFormatter", "JSONFormatter", "DSVFormatter", "KafkaStream" };
        azureHDInsightTemplates = new String[] { "OracleReader", "MysqlReader", "FileReader", "MSSqlReader", "HTTPReader", "TCPReader", "FreeFormTextParser", "AvroParser", "XMLParser", "JSONParser", "DSVParser", "HDInsightHDFSWriter", "HDInsightKafkaWriter", "SysOut", "FileWriter", "XMLFormatter", "AvroFormatter", "JSONFormatter", "DSVFormatter", "KafkaStream" };
        azureStorageTemplates = new String[] { "OracleReader", "MysqlReader", "MSSqlReader", "FileReader", "HTTPReader", "TCPReader", "FreeFormTextParser", "AvroParser", "XMLParser", "JSONParser", "DSVParser", "AzureBlobWriter", "SysOut", "FileWriter", "XMLFormatter", "AvroFormatter", "JSONFormatter", "DSVFormatter", "KafkaStream" };
        azureEventHubTemplates = new String[] { "OracleReader", "MysqlReader", "MSSqlReader", "FileReader", "HTTPReader", "TCPReader", "FreeFormTextParser", "AvroParser", "XMLParser", "JSONParser", "DSVParser", "JMSWriter", "SysOut", "FileWriter", "XMLFormatter", "AvroFormatter", "JSONFormatter", "DSVFormatter", "KafkaStream" };
        systemTemplates = new String[] { "MonitoringDataSource", "WebAlertAdapter" };
    }
    
    class ClusterMemberListener implements MembershipListener
    {
        public void memberAdded(final MembershipEvent membershipEvent) {
        }
        
        public void memberRemoved(final MembershipEvent membershipEvent) {
            final ILock lock = HazelcastSingleton.get().getLock("ServerList");
            lock.lock();
            try {
                final Member removedMember = (Member)membershipEvent.getMember();
                AppManagerServerLocation.removeAppManagerServerId(new UUID(removedMember.getUuid()));
                final MetaInfo.Server removedServer = (MetaInfo.Server)Server.this.metadataRepository.getMetaObjectByUUID(new UUID(removedMember.getUuid()), Server.this.sessionID);
                final Set<Member> members = HazelcastSingleton.get().getCluster().getMembers();
                final List<UUID> valid = new ArrayList<UUID>();
                for (final Member member : members) {
                    valid.add(new UUID(member.getUuid()));
                }
                final List<UUID> toRemove = new ArrayList<UUID>();
                final Set<MetaInfo.MetaObject> servers = (Set<MetaInfo.MetaObject>)Server.this.metadataRepository.getByEntityType(EntityType.SERVER, Server.this.sessionID);
                for (final MetaInfo.MetaObject obj : servers) {
                    final MetaInfo.Server server = (MetaInfo.Server)obj;
                    if (!valid.contains(server.uuid) && !server.isAgent) {
                        toRemove.add(server.uuid);
                    }
                }
                for (final UUID uuid : toRemove) {
                    if (Server.logger.isDebugEnabled()) {
                        Server.logger.debug((Object)("Trying to remove : " + uuid + " from \n" + valid));
                    }
                    Server.this.metadataRepository.removeMetaObjectByUUID(uuid, Server.this.sessionID);
                }
                RMIWebSocket.cleanupLeftUsers(membershipEvent.getMember().getUuid());
            }
            catch (MetaDataRepositoryException e) {
                Server.logger.error((Object)e);
            }
            catch (Throwable e2) {
                Server.logger.error((Object)("Failed to remove member with exception " + e2));
            }
            finally {
                lock.unlock();
            }
        }
        
        public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
        }
    }
    
    public static class AdhocCleanup implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -44213816144550014L;
        String consoleQueueName;
        
        public AdhocCleanup(final String consoleQueueName) {
            this.consoleQueueName = consoleQueueName;
        }
        
        @Override
        public Object call() throws Exception {
            return Server.server.adhocCleanup(this.consoleQueueName, null);
        }
    }
}
