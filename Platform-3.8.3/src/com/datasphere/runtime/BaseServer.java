package com.datasphere.runtime;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;
import org.jctools.queues.*;

import com.datasphere.distribution.HQueue;
import com.datasphere.exception.ServerException;
import com.datasphere.exceptionhandling.HExceptionMgr;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.Member;
import com.datasphere.messaging.MessagingProvider;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataDbFactory;
import com.datasphere.metaRepository.MetaDataDbProvider;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.proc.events.ShowStreamEvent;
import com.datasphere.runtime.channels.BroadcastAsyncChannel;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.channels.SimpleChannel;
import com.datasphere.runtime.channels.ZMQChannel;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.Link;
import com.datasphere.runtime.components.Publisher;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorApp;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

import javassist.Modifier;

public abstract class BaseServer implements HQueue.Listener, ServerServices
{
    public static final String GLOBAL_NAMSPACE = "Global";
    public static final UUID GLOBAL_NAMSPACE_UUID;
    public static final String MONITORING_SOURCE_APP = "MonitoringSourceApp";
    public static final String MONITORING_SOURCE_FLOW = "MonitoringSourceFlow";
    public static final String MONITORING_SOURCE = "MonitoringSource1";
    public static final String MONITORING_PROCESS_APP = "MonitoringProcessApp";
    private static String serverName;
    public static final String ADMIN = "admin";
    private static Logger logger;
    public static final long startupTimeStamp;
    public static MetaDataDbProvider metaDataDbProvider;
    private final ScheduledThreadPoolExecutor scheduler;
    public final Map<UUID, FlowComponent> openObjects;
    public MessagingSystem messagingSystem;
    public HExceptionMgr exception_manager;
    public MDRepository metadataRepository;
    private Stream showStream;
    public static volatile BaseServer baseServer;
    private Stream exceptionStream;
    long prevNumLogErrors;
    long prevNumLogWarns;
    long prevTimeStamp;
    Long prevCpuTime;
    private Logger monitorLogger;
    private MonitorLogAppender monitorAppender;
    private BlockingQueue<LoggingEvent> monitorLoggingEvents;
    
    public BaseServer() {
        this.prevNumLogErrors = -1L;
        this.prevNumLogWarns = -1L;
        this.prevTimeStamp = 0L;
        this.prevCpuTime = null;
        this.monitorLoggingEvents = new MpscCompoundQueue<LoggingEvent>(1024);
        this.openObjects = new ConcurrentHashMap<UUID, FlowComponent>();
        (this.scheduler = new ScheduledThreadPoolExecutor(4, new CustomThreadFactory("BaseServer_Scheduler"), new LogAndDiscardPolicy())).setRemoveOnCancelPolicy(true);
    }
    
    public static BaseServer getBaseServer() {
        return BaseServer.baseServer;
    }
    
    public static MetaDataDbProvider setMetaDataDbProviderDetails() {
        String dataBaseName = System.getProperty("com.datasphere.config.metaDataRepositoryDB");
        if (dataBaseName == null || dataBaseName.isEmpty()) {
            dataBaseName = "derby";
        }
        return BaseServer.metaDataDbProvider = MetaDataDbFactory.getOurMetaDataDb(dataBaseName);
    }
    
    public static MetaDataDbProvider getMetaDataDBProviderDetails() {
        return BaseServer.metaDataDbProvider;
    }
    
    public void initializeMetaData() {
        this.metadataRepository = MetadataRepository.getINSTANCE();
        String candidateServerName = null;
        try {
            candidateServerName = generateServerName();
        }
        catch (Exception e) {
            BaseServer.logger.error((Object)e.getMessage());
            System.exit(0);
        }
        BaseServer.serverName = candidateServerName;
    }
    
    public void initializeMessaging() {
        this.messagingSystem = MessagingProvider.getMessagingSystem("com.datasphere.jmqmessaging.ZMQSystem");
    }
    
    public MessagingSystem getMessagingSystem() {
        return this.messagingSystem;
    }
    
    public Stream getExceptionStream() throws Exception {
        return this.exceptionStream;
    }
    
    public Stream getShowStream() {
        return this.showStream;
    }
    
    protected MetaInfo.Type getTypeForCLass(final Class<?> clazz) throws MetaDataRepositoryException {
        MetaInfo.Type type = null;
        final String typeName = "Global." + clazz.getSimpleName();
        try {
            type = (MetaInfo.Type)this.metadataRepository.getMetaObjectByName(EntityType.TYPE, "Global", clazz.getSimpleName(), null, HSecurityManager.TOKEN);
        }
        catch (Exception e) {
            if (BaseServer.logger.isInfoEnabled()) {
                BaseServer.logger.info((Object)e.getLocalizedMessage());
            }
            return null;
        }
        if (type == null) {
            final Map<String, String> fields = new LinkedHashMap<String, String>();
            final Field[] declaredFields;
            final Field[] cFields = declaredFields = clazz.getDeclaredFields();
            for (final Field f : declaredFields) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            type = new MetaInfo.Type();
            type.construct(typeName, MetaInfo.GlobalNamespace, clazz.getName(), fields, null, false);
            this.putObject(type);
        }
        return type;
    }
    
    public void initExceptionHandler() {
        try {
            if (BaseServer.logger.isInfoEnabled()) {
                BaseServer.logger.info((Object)"creating exception manager");
            }
            this.exception_manager = HExceptionMgr.get();
            final MetaInfo.Type dataType = this.getTypeForCLass(ExceptionEvent.class);
            final MetaInfo.Stream streamMetaObj = new MetaInfo.Stream();
            streamMetaObj.construct("exceptionsStream", HExceptionMgr.ExceptionStreamUUID, MetaInfo.GlobalNamespace, dataType.uuid, null, null, null);
            this.metadataRepository.putMetaObject(streamMetaObj, HSecurityManager.TOKEN);
            (this.exceptionStream = (Stream)this.putOpenObjectIfNotExists(streamMetaObj.uuid, new StreamObjectFac() {
                @Override
                public FlowComponent create() throws Exception {
                    final Stream s = new Stream(streamMetaObj, BaseServer.this, null);
                    return s;
                }
            })).start();
            this.subscribe(this.exceptionStream, this.exception_manager);
            if (BaseServer.logger.isDebugEnabled()) {
                BaseServer.logger.debug((Object)"exception stream, control stream are created");
            }
        }
        catch (Exception se) {
            BaseServer.logger.error((Object)("error" + se));
            se.printStackTrace();
        }
    }
    
    public void closeExceptionStream() {
        try {
            this.unsubscribe(this.exceptionStream, this.exception_manager);
            this.exceptionStream.close();
        }
        catch (Exception e) {
            BaseServer.logger.warn((Object)("Error in closing ExceptionStream : " + e.getMessage()));
        }
    }
    
    public void initShowStream() {
        try {
            if (BaseServer.logger.isInfoEnabled()) {
                BaseServer.logger.info((Object)"Creating SHOW Stream");
            }
            final MetaInfo.Type dataType = this.getTypeForCLass(ShowStreamEvent.class);
            final MetaInfo.Stream streamMetaObj = new MetaInfo.Stream();
            streamMetaObj.construct("showStream", ShowStreamManager.ShowStreamUUID, MetaInfo.GlobalNamespace, dataType.uuid, null, null, null);
            (this.showStream = (Stream)this.putOpenObjectIfNotExists(streamMetaObj.uuid, new StreamObjectFac() {
                @Override
                public FlowComponent create() throws Exception {
                    final Stream s = new Stream(streamMetaObj, BaseServer.this, null);
                    return s;
                }
            })).start();
            if (BaseServer.logger.isDebugEnabled()) {
                BaseServer.logger.debug((Object)"exception stream, control stream are created");
            }
        }
        catch (Exception se) {
            BaseServer.logger.error((Object)("error" + se));
            se.printStackTrace();
        }
    }
    
    public void initMonitoringApp() throws MetaDataRepositoryException {
        this.monitorLogger = Logger.getRootLogger();
        (this.monitorAppender = new MonitorLogAppender(this)).setThreshold((Priority)Level.WARN);
        this.monitorLogger.addAppender((Appender)this.monitorAppender);
        MonitorApp.getMonitorApp();
    }
    
    public MetaInfo.MetaObject getMetaObject(final UUID uuid) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
    }
    
    public MetaInfo.Type getTypeInfo(final UUID uuid) throws ServerException, MetaDataRepositoryException {
        return this.getObjectInfo(uuid, EntityType.TYPE);
    }
    
    public MetaInfo.DeploymentGroup getDeploymentGroupByName(final String name) throws MetaDataRepositoryException {
        return (MetaInfo.DeploymentGroup)this.getObject(EntityType.DG, "Global", name);
    }
    
    protected MetaInfo.MetaObject putObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.metadataRepository.putMetaObject(obj, HSecurityManager.TOKEN);
        return this.metadataRepository.getMetaObjectByUUID(obj.getUuid(), HSecurityManager.TOKEN);
    }
    
    protected MetaInfo.MetaObject updateObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.metadataRepository.updateMetaObject(obj, HSecurityManager.TOKEN);
        return this.metadataRepository.getMetaObjectByUUID(obj.getUuid(), HSecurityManager.TOKEN);
    }
    
    protected MetaInfo.PropertyTemplateInfo getPropertyTemplateByNameAndVersion(final String name, final String version) throws MetaDataRepositoryException {
        final Set<MetaInfo.PropertyTemplateInfo> ptSet = (Set<MetaInfo.PropertyTemplateInfo>)this.metadataRepository.getByEntityType(EntityType.PROPERTYTEMPLATE, HSecurityManager.TOKEN);
        for (final MetaInfo.PropertyTemplateInfo pt : ptSet) {
            if (pt.name.equals(name) && pt.adapterVersion.equals(version)) {
                return pt;
            }
        }
        return null;
    }
    
    protected MetaInfo.MetaObject getObject(final EntityType type, final String namespace, final String name) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByName(type, namespace, name, null, HSecurityManager.TOKEN);
    }
    
    public MetaInfo.MetaObject getObject(final UUID uuid) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
    }
    
    public MetaInfo.DeploymentGroup getDeploymentGroupByID(final UUID uuid) throws ServerException, MetaDataRepositoryException {
        return this.getObjectInfo(uuid, EntityType.DG);
    }
    
    public void subscribe(final Publisher pub, final Subscriber sub) throws Exception {
        this.subscribe(pub, new Link(sub));
    }
    
    public void subscribe(final Publisher pub, final Link link) throws Exception {
        final Channel c = pub.getChannel();
        this.subscribe(c, link);
    }
    
    public void subscribe(final Channel c, final Link link) throws Exception {
        c.addSubscriber(link);
    }
    
    public void unsubscribe(final Publisher pub, final Subscriber sub) throws Exception {
        this.unsubscribe(pub, new Link(sub));
    }
    
    public void unsubscribe(final Publisher pub, final Link link) throws Exception {
        final Channel c = pub.getChannel();
        if (c != null) {
            this.unsubscribe(c, link);
        }
    }
    
    public void unsubscribe(final Channel c, final Link link) throws Exception {
        c.removeSubscriber(link);
    }
    
    public Channel createSimpleChannel() {
        return new SimpleChannel();
    }
    
    public MetaInfo.Stream getStreamInfo(final UUID uuid) throws ServerException, MetaDataRepositoryException {
        return this.getObjectInfo(uuid, EntityType.STREAM);
    }
    
    @Override
    public ScheduledThreadPoolExecutor getScheduler() {
        return this.scheduler;
    }
    
    public void shutdown() {
        this.scheduler.shutdown();
        MessagingProvider.shutdownAll();
        try {
            if (!this.scheduler.awaitTermination(5L, TimeUnit.SECONDS)) {
                this.scheduler.shutdownNow();
                if (!this.scheduler.awaitTermination(60L, TimeUnit.SECONDS)) {
                    System.err.println("Scheduler did not terminate");
                }
            }
        }
        catch (InterruptedException ie) {
            this.scheduler.shutdown();
            Thread.currentThread().interrupt();
        }
    }
    
    public void putOpenObject(final FlowComponent obj) throws MetaDataRepositoryException {
        final UUID id = obj.getMetaID();
        synchronized (this.openObjects) {
            this.openObjects.put(id, obj);
            final Pair pair = Pair.make(id, this.getServerID());
            this.metadataRepository.putDeploymentInfo(pair, HSecurityManager.TOKEN);
        }
    }
    
    protected FlowComponent putOpenObjectIfNotExists(final UUID objId, final StreamObjectFac fac) throws Exception {
        synchronized (this.openObjects) {
            FlowComponent obj = this.openObjects.get(objId);
            if (obj == null) {
                obj = fac.create();
                if (!this.openObjects.containsKey(objId)) {
                    this.putOpenObject(obj);
                }
                else {
                    assert this.openObjects.containsKey(objId);
                }
            }
            return obj;
        }
    }
    
    public FlowComponent getOpenObject(final UUID uuid) {
        synchronized (this.openObjects) {
            return this.openObjects.get(uuid);
        }
    }
    
    protected void closeOpenObject(final FlowComponent object) throws Exception {
        final UUID id = object.getMetaID();
        final String uri = object.getMetaUri();
        try {
            object.close();
        }
        finally {
            if (BaseServer.logger.isInfoEnabled()) {
                BaseServer.logger.info((Object)("Closed " + uri));
            }
            this.metadataRepository.removeDeploymentInfo(Pair.make(id, this.getServerID()), HSecurityManager.TOKEN);
        }
    }
    
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        final Collection<MonitorEvent> monEvs = new ArrayList<MonitorEvent>();
        final UUID entityID;
        final UUID serverID = entityID = this.getServerID();
        final long totalMemory = Runtime.getRuntime().totalMemory();
        final long maxMemory = Runtime.getRuntime().maxMemory();
        final long freeMemory = maxMemory - (totalMemory - Runtime.getRuntime().freeMemory());
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.UPTIME, Long.valueOf(ts - BaseServer.startupTimeStamp), Long.valueOf(ts)));
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.MEMORY_TOTAL, Long.valueOf(totalMemory), Long.valueOf(ts)));
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.MEMORY_MAX, Long.valueOf(maxMemory), Long.valueOf(ts)));
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.MEMORY_FREE, Long.valueOf(freeMemory), Long.valueOf(ts)));
        try {
            final java.lang.management.OperatingSystemMXBean mx = ManagementFactory.getOperatingSystemMXBean();
            final OperatingSystemMXBean mxb = (OperatingSystemMXBean)mx;
            final long cpuTime = mxb.getProcessCpuTime();
            if (cpuTime < 0L) {
                monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, "Unavailable", Long.valueOf(ts)));
            }
            else if (this.prevCpuTime != null) {
                final long cpuDelta = Math.abs(cpuTime - this.prevCpuTime);
                final int cores = Runtime.getRuntime().availableProcessors();
                final Long cpuRate = 1000L * cpuDelta / (ts - this.prevTimeStamp);
                monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, cpuRate, Long.valueOf(ts)));
                final String cpuRateRatePerNode = String.format("%2.1f%%", cpuRate / 1.0E9 / cores * 100.0);
                monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_PER_NODE, cpuRateRatePerNode, Long.valueOf(ts)));
            }
            this.prevCpuTime = cpuTime;
        }
        catch (UnsupportedOperationException e) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, "Not supported", Long.valueOf(ts)));
        }
        catch (Exception e2) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, "Unavailable", Long.valueOf(ts)));
        }
        try {
            final File[] roots = File.listRoots();
            final StringBuilder sb = new StringBuilder();
            for (final File root : roots) {
                sb.append(root.toString()).append(": ").append(root.getFreeSpace() / 1000000000L).append("GB");
                if (root != roots[roots.length - 1]) {
                    sb.append("; ");
                }
            }
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.DISK_FREE, sb.toString(), Long.valueOf(ts)));
        }
        catch (UnsupportedOperationException e) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, "Not supported", Long.valueOf(ts)));
        }
        catch (Exception e2) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CPU_RATE, "Unavailable", Long.valueOf(ts)));
        }
        final int cores2 = Runtime.getRuntime().availableProcessors();
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CORES, Long.valueOf(cores2), Long.valueOf(ts)));
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.MAC_ADDRESS, HazelcastSingleton.getMacID(), Long.valueOf(ts)));
        monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.VERSION, Version.getVersionString(), Long.valueOf(ts)));
        if (this instanceof Server) {
            final Server server = (Server)this;
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.CLUSTER_NAME, server.ServerInfo.getInitializer().WAClusterName, Long.valueOf(ts)));
        }
        LoggingEvent monLoggingEvent = null;
        long numLogErrors = 0L;
        long numLogWarns = 0L;
        while ((monLoggingEvent = this.monitorLoggingEvents.peek()) != null && monLoggingEvent.timeStamp <= ts) {
            try {
                monLoggingEvent = this.monitorLoggingEvents.take();
            }
            catch (InterruptedException ex) {}
            String logValue = monLoggingEvent.getLoggerName() + ": ";
            if (monLoggingEvent.locationInformationExists()) {
                logValue = logValue + monLoggingEvent.getLocationInformation().fullInfo + ": ";
            }
            logValue += monLoggingEvent.getMessage();
            if (logValue.length() > 4096) {
                logValue = logValue.substring(0, 4092) + "...";
            }
            MonitorEvent.Type logType;
            if (monLoggingEvent.getLevel().toInt() >= Level.ERROR.toInt()) {
                logType = MonitorEvent.Type.LOG_ERROR;
                ++numLogErrors;
            }
            else {
                logType = MonitorEvent.Type.LOG_WARN;
                ++numLogWarns;
            }
            monEvs.add(new MonitorEvent(serverID, entityID, logType, logValue, Long.valueOf(ts)));
        }
        if (numLogErrors > 0L || this.prevNumLogErrors != 0L) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.NUM_LOG_ERRORS, Long.valueOf(numLogErrors), Long.valueOf(ts)));
            this.prevNumLogErrors = numLogErrors;
        }
        if (numLogWarns > 0L || this.prevNumLogWarns != 0L) {
            monEvs.add(new MonitorEvent(serverID, entityID, MonitorEvent.Type.NUM_LOG_WARNS, Long.valueOf(numLogWarns), Long.valueOf(ts)));
            this.prevNumLogWarns = numLogWarns;
        }
        this.prevTimeStamp = ts;
        return monEvs;
    }
    
    public abstract boolean isServer();
    
    private static String generateServerName() throws Exception {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final ISemaphore lock = hz.getSemaphore("HD Node Name Generator Semaphore");
        lock.init(1);
        lock.acquire();
        try {
            final String setName = System.getProperty("com.datasphere.config.server.name");
            if (setName != null && !setName.isEmpty()) {
                if (serverNameIsInUse(hz, setName)) {
                    throw new Exception("Server name already in use: " + setName);
                }
                if (setName.contains(".")) {
                    throw new Exception("Server name must not contain a period: " + setName);
                }
                return setName;
            }
            else {
                final InetSocketAddress adr = (InetSocketAddress)HazelcastSingleton.get().getLocalEndpoint().getSocketAddress();
                final String address = adr.getAddress().getHostAddress().toString().replace('.', '-');
                final String groupsString = System.getProperty("com.datasphere.deploymentGroups");
                final String serverOrAgent = HazelcastSingleton.isClientMember() ? "A" : "S";
                final String[] groups = (groupsString == null) ? new String[0] : groupsString.split(",");
                Arrays.sort(groups);
                final StringBuilder sb = new StringBuilder();
                sb.append(serverOrAgent);
                sb.append(address);
                for (final String group : groups) {
                    if (!group.equalsIgnoreCase("default")) {
                        sb.append("_");
                        sb.append(group.trim());
                    }
                }
                String baseName = sb.toString().replaceAll("[^A-Za-z0-9]+", "_");
                if (!serverNameIsInUse(hz, baseName)) {
                    return baseName;
                }
                baseName += "_";
                for (int i = 2; i <= 99; ++i) {
                    final String fullName = baseName + i;
                    if (!serverNameIsInUse(hz, fullName)) {
                        return fullName;
                    }
                }
                throw new Exception("Unable to generate a unique name similar to " + baseName + "1");
            }
        }
        finally {
            lock.release();
        }
    }
    
    public static boolean serverNameIsInUse(final HazelcastInstance hz, final String serverName) {
        final Set<Member> allServersAndAgents = (Set<Member>)hz.getCluster().getMembers();
        final RemoteCall<String> getServerNameCallable = new RemoteCall<String>() {
            private static final long serialVersionUID = 4152339478021968346L;
            
            @Override
            public String call() {
                return BaseServer.getServerName();
            }
        };
        final Collection<String> namesInUse = DistributedExecutionManager.exec(hz, getServerNameCallable, allServersAndAgents);
        for (final String nameInUse : namesInUse) {
            if (serverName.equals(nameInUse)) {
                return true;
            }
        }
        return false;
    }
    
    public static String getServerName() {
        return BaseServer.serverName;
    }
    
    public BroadcastAsyncChannel createChannel(final FlowComponent owner) {
        return new BroadcastAsyncChannel(owner);
    }
    
    public abstract Collection<FlowComponent> getAllObjectsView();
    
    public abstract Collection<Channel> getAllChannelsView();
    
    public abstract Stream getStream(final UUID p0, final Flow p1) throws Exception;
    
    @Override
    public abstract ExecutorService getThreadPool();
    
    public abstract boolean inDeploymentGroup(final UUID p0);
    
    public abstract UUID getServerID();
    
    public abstract List<String> getDeploymentGroups();
    
    public abstract <T extends MetaInfo.MetaObject> T getObjectInfo(final UUID p0, final EntityType p1) throws ServerException, MetaDataRepositoryException;
    
    public abstract ZMQChannel getDistributedChannel(final Stream p0);
    
    static {
        GLOBAL_NAMSPACE_UUID = new UUID("9A315652-1204-6E32-9105-AEC16CC6AD49");
        BaseServer.serverName = "__server";
        BaseServer.logger = Logger.getLogger((Class)BaseServer.class);
        startupTimeStamp = System.currentTimeMillis();
    }
    
    public static class LogAndDiscardPolicy implements RejectedExecutionHandler
    {
        @Override
        public void rejectedExecution(final Runnable task, final ThreadPoolExecutor executor) {
            BaseServer.logger.warn((Object)("Task rejected from BaseServer.scheduler: Task: " + task.getClass().getName() + " ; Executor status: " + executor.toString()));
        }
    }
    
    private static class MonitorLogAppender extends AppenderSkeleton
    {
        private final BaseServer parent;
        
        MonitorLogAppender(final BaseServer parent) {
            this.parent = parent;
        }
        
        protected void append(final LoggingEvent loggingEvent) {
            try {
                this.parent.monitorLoggingEvents.add(loggingEvent);
            }
            catch (IllegalStateException ex) {}
        }
        
        public void close() {
        }
        
        public boolean requiresLayout() {
            return false;
        }
    }
    
    protected interface StreamObjectFac
    {
        FlowComponent create() throws Exception;
    }
}
