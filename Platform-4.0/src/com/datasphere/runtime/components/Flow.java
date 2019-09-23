package com.datasphere.runtime.components;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.DisapproveQuiesceException;
import com.datasphere.appmanager.NodeManager;
import com.datasphere.appmanager.event.Event;
import com.datasphere.exception.ServerException;
import com.datasphere.historicalcache.Cache;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.persistence.HStore;
import com.datasphere.proc.events.commands.CheckpointCommandEvent;
import com.datasphere.proc.events.commands.FlushCommandEvent;
import com.datasphere.proc.events.commands.QuiesceCheckpointCommandEvent;
import com.datasphere.recovery.AfterSourcePosition;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.DeploymentStrategy;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.ReportStats;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.channels.KafkaChannel;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.runtime.window.Window;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.DeployUtility;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Flow extends FlowComponent implements Restartable, FlowStateApi
{
    private static Logger logger;
    private final Flow parent;
    public long epochNumber;
    private volatile boolean running;
    private final Server srv;
    public final MetaInfo.Flow flowInfo;
    private final List<IStreamGenerator> gens;
    private final List<Source> sources;
    private final List<Target> targets;
    private final List<CQTask> cqs;
    private final List<IWindow> windows;
    private final List<Sorter> sorters;
    private final List<Cache> caches;
    private final List<HStore> wastores;
    private final List<HStoreView> wastoreviews;
    private final List<Flow> flows;
    private final Map<UUID, Stream> usedStreams;
    private final List<UUID> deployedFlows;
    private final List<Compound> compoundComponents;
    private Long recoveryPeriod;
    private Integer recoveryType;
    private MDRepository mdRepository;
    private NodeManager nodeManager;
    private Map<UUID, Integer> indegrees;
    private List<MultiComponent> multiComponents;
    
    public Flow(final MetaInfo.Flow flowInfo, final BaseServer srv, final Flow parent) throws Exception {
        super(srv, flowInfo);
        this.epochNumber = -1L;
        this.running = false;
        this.gens = new ArrayList<IStreamGenerator>();
        this.sources = new ArrayList<Source>();
        this.targets = new ArrayList<Target>();
        this.cqs = new ArrayList<CQTask>();
        this.windows = new ArrayList<IWindow>();
        this.sorters = new ArrayList<Sorter>();
        this.caches = new ArrayList<Cache>();
        this.wastores = new ArrayList<HStore>();
        this.wastoreviews = new ArrayList<HStoreView>();
        this.flows = new ArrayList<Flow>();
        this.usedStreams = new HashMap<UUID, Stream>();
        this.deployedFlows = new ArrayList<UUID>();
        this.compoundComponents = new ArrayList<Compound>();
        this.recoveryPeriod = null;
        this.recoveryType = null;
        this.mdRepository = MetadataRepository.getINSTANCE();
        this.multiComponents = new ArrayList<MultiComponent>(0);
        this.parent = parent;
        this.srv = (Server)srv;
        this.flowInfo = flowInfo;
        for (final UUID streamID : this.getListOfReferredStreams()) {
            this.usedStreams.put(streamID, null);
        }
    }
    
    @Override
    public void addSessionToReport(final AuthToken token) {
        final ReportStats.FlowReportStats flStats = new ReportStats.FlowReportStats(this.getMetaInfo());
        this.compStats.put(token, flStats);
        for (final Flow flobj : this.flows) {
            flobj.addSessionToReport(token);
        }
        for (final Source src : this.sources) {
            src.addSessionToReport(token);
        }
        for (final HStore ws : this.wastores) {
            ws.addSessionToReport(token);
        }
        for (final Target tgt : this.targets) {
            tgt.addSessionToReport(token);
        }
    }
    
    @Override
    public ReportStats.BaseReportStats removeSessionToReport(final AuthToken token, final boolean computeRetVal) {
        final ReportStats.FlowReportStats stats = (ReportStats.FlowReportStats)this.compStats.remove(token);
        if (stats == null) {
            return null;
        }
        for (final Flow flobj : this.flows) {
            final ReportStats.FlowReportStats flstats = (ReportStats.FlowReportStats)flobj.removeSessionToReport(token, computeRetVal);
            if (computeRetVal) {
                stats.srcStats.addAll(flstats.srcStats);
                stats.tgtStats.addAll(flstats.tgtStats);
                stats.wsStats.addAll(flstats.wsStats);
                if (flstats.getCheckpointTS() == -1L || flstats.getCheckpointTS() <= stats.getCheckpointTS()) {
                    continue;
                }
                stats.setCheckpointTS(flstats.getCheckpointTS());
            }
        }
        for (final Source src : this.sources) {
            final ReportStats.SourceReportStats srcStats = (ReportStats.SourceReportStats)src.removeSessionToReport(token, computeRetVal);
            if (computeRetVal) {
                stats.srcStats.add(srcStats);
            }
        }
        for (final HStore ws : this.wastores) {
            final ReportStats.HDstoreReportStats wsStats = (ReportStats.HDstoreReportStats)ws.removeSessionToReport(token, computeRetVal);
            if (computeRetVal) {
                stats.wsStats.add(wsStats);
            }
        }
        for (final Target tgt : this.targets) {
            final ReportStats.TargetReportStats tgtStats = (ReportStats.TargetReportStats)tgt.removeSessionToReport(token, computeRetVal);
            if (computeRetVal) {
                stats.tgtStats.add(tgtStats);
            }
        }
        return stats;
    }
    
    public void updateSessionStats() {
        for (final ReportStats.BaseReportStats stats : this.compStats.values()) {
            final ReportStats.FlowReportStats flStats = (ReportStats.FlowReportStats)stats;
            final long eventTS = System.currentTimeMillis();
            flStats.setCheckpointTS(eventTS);
        }
    }
    
    @Override
    public Flow getTopLevelFlow() {
        if (this.getMetaType() == EntityType.APPLICATION) {
            return this;
        }
        return super.getTopLevelFlow();
    }
    
    @Override
    public Flow getFlow() {
        if (this.getMetaType() == EntityType.APPLICATION) {
            return this;
        }
        return super.getFlow();
    }
    
    public boolean startSessionReporting(final AuthToken token) {
        for (final Source srcObj : this.sources) {
            srcObj.addSessionToReport(token);
        }
        return true;
    }
    
    public void startNodeManager() throws Exception {
        this.nodeManager = new NodeManager(this, this.srv);
    }
    
    public NodeManager getNodeManager() {
        return this.nodeManager;
    }
    
    private void getStreamsInObject(final UUID id, final Set<UUID> result) throws Exception {
        final MetaInfo.MetaObject obj = this.srv.getObject(id);
        if (obj != null) {
            if (obj instanceof MetaInfo.Stream) {
                result.add(obj.uuid);
            }
            else {
                for (final UUID dep : obj.getDependencies()) {
                    this.getStreamsInObject(dep, result);
                }
            }
        }
    }
    
    private Collection<UUID> getListOfReferredStreams() throws Exception {
        final Set<UUID> result = new HashSet<UUID>();
        for (final UUID dep : this.getMetaDependencies()) {
            this.getStreamsInObject(dep, result);
        }
        return result;
    }
    
    private void stopObjects(final MetaInfo.MetaObject mo) throws Exception {
        final FlowComponent o = this.srv.getOpenObject(mo.getUuid());
        if (o == null) {
            return;
        }
        switch (mo.getType()) {
            case STREAM: {
                final UUID streamUUID = mo.getUuid();
                if (this.flowInfo != null && this.flowInfo.objects != null && this.flowInfo.objects.containsKey(EntityType.STREAM) && this.flowInfo.objects.get(EntityType.STREAM).contains(streamUUID)) {
                    ((Stream)o).stop();
                    break;
                }
                break;
            }
            case CQ: {
                assert mo.getType() == EntityType.CQ;
                o.stop();
                break;
            }
            case SORTER: {
                o.stop();
                break;
            }
            case SOURCE: {
                o.stop();
                break;
            }
            case STREAM_GENERATOR: {
                o.stop();
                break;
            }
            case TARGET: {
                o.stop();
                break;
            }
            case HDSTORE: {
                o.stop();
                break;
            }
            case WASTOREVIEW: {
                o.stop();
                break;
            }
            case WINDOW: {
                o.stop();
                break;
            }
        }
    }
    
    @Override
    public synchronized void start() throws Exception {
        this.nodeManager.clearWaiting();
        this.setFlowsAndCheckpoints(null);
        final List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = this.getFlowMeta().showTopology();
        Collections.reverse(orderWithFlowInfo);
        for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo) {
            final Flow f = (Flow)this.srv.getOpenObject(pair.second.getUuid());
            if (f != null) {
                f.start(pair.first);
            }
        }
    }
    
    public synchronized void start(final List<UUID> servers) throws Exception {
        final Map<UUID, Set<UUID>> serverToDeployedObjects = filterServerToDeployedList(servers, this.getMetaName());
        this.setFlowsAndCheckpoints(serverToDeployedObjects);
        final List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = this.getFlowMeta().showTopology();
        Collections.reverse(orderWithFlowInfo);
        for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo) {
            final Flow f = (Flow)this.srv.getOpenObject(pair.second.getUuid());
            if (f != null && this.deployedFlows.contains(pair.second.getUuid())) {
                f.start(pair.first, serverToDeployedObjects);
            }
        }
    }
    
    void setFlowsAndCheckpoints(final Map<UUID, Set<UUID>> serverToDeployedObjects) throws Exception {
        if (this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot start streams for flow " + this.getMetaName() + " because it is already running"));
            }
            return;
        }
        this.running = true;
        try {
            for (final Stream stream : this.usedStreams.values()) {
                if (stream != null) {
                    stream.start(serverToDeployedObjects);
                }
            }
            for (final Flow s : this.flows) {
                s.setFlowsAndCheckpoints(serverToDeployedObjects);
            }
        }
        catch (Exception e) {
            this.stopAllComponents();
            this.running = false;
            throw e;
        }
    }
    
    public synchronized void start(final MetaInfo.MetaObject obj, final Map<UUID, Set<UUID>> serverToDeployedObjects) throws Exception {
        if (obj.getType().equals(EntityType.STREAM)) {
            final FlowComponent o = this.srv.getOpenObject(obj.getUuid());
            if (o != null) {
                ((Stream)o).start(serverToDeployedObjects);
            }
        }
        else {
            this.start(obj);
        }
    }
    
    public synchronized void start(final MetaInfo.MetaObject obj) throws Exception {
        Position appCheckpoint = null;
        PathManager egressCheckpoint = null;
        PathManager mergedCheckpoint = null;
        try {
            final FlowComponent o = this.srv.getOpenObject(obj.getUuid());
            switch (obj.getType()) {
                case STREAM: {
                    ((Stream)o).start();
                    break;
                }
                case CACHE: {
                    ((Cache)o).start();
                    break;
                }
                case CQ: {
                    if (this.getRecoveryType() == 2) {
                        if (appCheckpoint == null) {
                            appCheckpoint = Utility.updateUuids(StatusDataStore.getInstance().getAppCheckpoint(this.flowInfo.getCurrentApp().uuid));
                        }
                        if (appCheckpoint != null) {
                            final Position p = appCheckpoint.getLowPositionForComponent(o.getMetaID());
                            if (!p.isEmpty()) {
                                if (Logger.getLogger("Recovery").isInfoEnabled()) {
                                    Logger.getLogger("Recovery").info((Object)("Repositioning CQ " + o.getMetaName() + " to " + p));
                                    Utility.prettyPrint(p);
                                }
                                ((CQTask)o).setWaitPosition(p);
                            }
                        }
                    }
                    ((CQTask)o).start();
                    break;
                }
                case SORTER: {
                    ((Sorter)o).start();
                }
                case SOURCE: {}
                case TARGET: {
                    final MetaInfo.Target target = (MetaInfo.Target)obj;
                    final Map parallelismProperties = target.getParallelismProperties();
                    if (parallelismProperties != null) {
                        ((MultiComponent)o).start();
                        break;
                    }
                    ((Target)o).start();
                    break;
                }
                case HDSTORE: {
                    ((HStore)o).start();
                    break;
                }
                case WASTOREVIEW: {
                    ((HStoreView)o).start();
                    break;
                }
                case WINDOW: {
                    if (this.getRecoveryType() == 2) {
                        if (appCheckpoint == null) {
                            appCheckpoint = Utility.updateUuids(StatusDataStore.getInstance().getAppCheckpoint(this.flowInfo.getCurrentApp().uuid));
                        }
                        if (egressCheckpoint == null) {
                            egressCheckpoint = this.getEndpointCheckpoint();
                        }
                        if (mergedCheckpoint == null) {
                            mergedCheckpoint = new PathManager();
                            if (appCheckpoint != null) {
                                mergedCheckpoint.mergeWiderPosition(appCheckpoint.values());
                            }
                            if (egressCheckpoint != null) {
                                final Collection<Path> verifiedAfterPaths = new HashSet<Path>();
                                for (final Path egressPath : egressCheckpoint.values()) {
                                    final Path verifiedAfterPath = egressPath.isAfter() ? egressPath : new Path(egressPath.getPathItems(), egressPath.getLowSourcePosition(), egressPath.getHighSourcePosition(), "^");
                                    verifiedAfterPaths.add(verifiedAfterPath);
                                }
                                mergedCheckpoint.mergeWiderPosition((Collection)verifiedAfterPaths);
                            }
                            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                                Logger.getLogger("Recovery").debug((Object)("Flow " + this.flowInfo.name + " building the Merged Checkpoint from the AC and the WSC"));
                                Logger.getLogger("Recovery").debug((Object)"    ...the App Checkpoint:");
                                if (appCheckpoint != null) {
                                    Utility.prettyPrint(appCheckpoint);
                                }
                                Logger.getLogger("Recovery").debug((Object)"    ...the Egress Checkpoint:");
                                if (egressCheckpoint != null) {
                                    Utility.prettyPrint(egressCheckpoint);
                                }
                                Logger.getLogger("Recovery").debug((Object)"    ...the Merged Checkpoint:");
                                if (mergedCheckpoint != null) {
                                    Utility.prettyPrint(mergedCheckpoint);
                                }
                            }
                        }
                        if (egressCheckpoint != null && o instanceof Window) {
                            final Window win = (Window)o;
                            final Position pMC = mergedCheckpoint.getLowPositionForComponent(o.getMetaID()).toPosition();
                            if (!pMC.isEmpty()) {
                                win.setWaitPosition(pMC);
                            }
                        }
                    }
                    ((IWindow)o).start();
                    break;
                }
            }
        }
        catch (Exception e) {
            this.stopAllComponents();
            this.running = false;
            throw e;
        }
    }
    
    public boolean verifyAppStart(final List<UUID> servers) throws MetaDataRepositoryException {
        final Map<UUID, Set<UUID>> serverToDeployedObjects = filterServerToDeployedList(servers, this.getMetaName());
        return this.verifyStart(serverToDeployedObjects);
    }
    
    private static Map<UUID, Set<UUID>> filterServerToDeployedList(final List<UUID> servers, final String flowName) throws MetaDataRepositoryException {
        final Map<UUID, Set<UUID>> serverToDeployedObjects = new HashMap<UUID, Set<UUID>>();
        final Set<MetaInfo.Server> serversMetaInfo = (Set<MetaInfo.Server>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, HSecurityManager.TOKEN);
        for (final MetaInfo.Server srv : serversMetaInfo) {
            if (servers.contains(srv.getUuid())) {
                final Map<String, Set<UUID>> srvObjectUUIDs = srv.getCurrentUUIDs();
                final Set<UUID> deployedObjects = srvObjectUUIDs.get(flowName);
                serverToDeployedObjects.put(srv.getUuid(), deployedObjects);
            }
        }
        return serverToDeployedObjects;
    }
    
    public boolean verifyStart(final Map<UUID, Set<UUID>> serverToDeployedObjects) throws MetaDataRepositoryException {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot verify start for flow " + this.getMetaName() + " because it is not running"));
            }
            return false;
        }
        for (final Stream stream : this.usedStreams.values()) {
            if (stream != null && !stream.verifyStart(serverToDeployedObjects)) {
                return false;
            }
        }
        for (final Flow s : this.flows) {
            if (!s.verifyStart(serverToDeployedObjects)) {
                return false;
            }
        }
        return true;
    }
    
    public synchronized void startSources(final List<UUID> servers) throws Exception {
        try {
            final Position ac = Utility.updateUuids(StatusDataStore.getInstance().getAppCheckpoint(this.flowInfo.getCurrentApp().uuid));
            final PathManager appCheckpoint = new PathManager((ac != null) ? ac.values() : null);
            if (Logger.getLogger("Recovery").isInfoEnabled()) {
                Logger.getLogger("Recovery").info((Object)("Flow " + this.flowInfo.name + " on server " + BaseServer.getServerName() + " restarting sources at position " + appCheckpoint));
                if (appCheckpoint != null) {
                    Utility.prettyPrint(appCheckpoint);
                }
            }
            final boolean sendPositions = this.recoveryIsEnabled();
            for (final Source s : this.sources) {
                SourcePosition sp;
                if (s.requiresPartitionedSourcePosition()) {
                    sp = (SourcePosition)((appCheckpoint != null) ? appCheckpoint.getPartitionedSourcePositionForComponent(s.getMetaID()) : null);
                }
                else {
                    final Path p = (appCheckpoint != null) ? appCheckpoint.getLowSourcePositionForComponent(s.getMetaID()) : null;
                    if (p == null) {
                        sp = null;
                    }
                    else if (p.getAtOrAfter() == "^") {
                        sp = (SourcePosition)new AfterSourcePosition(p.getLowSourcePosition());
                    }
                    else {
                        sp = p.getLowSourcePosition();
                    }
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("Source " + s.getMetaName() + " (" + s.getMetaID() + ") set restart position to: " + sp));
                }
                s.setPosition(sp);
                s.setSendPositions(sendPositions);
                s.setOwnerFlow(this);
                if (servers != null) {
                    final List<UUID> relevantServers = findServersWithThisFlow(servers, this.flowInfo.getUuid());
                    s.serversWithDeployedComponent(relevantServers);
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("Starting source " + s.getMetaName() + " at position " + sp));
                }
                s.start();
            }
            for (final Stream s2 : this.usedStreams.values()) {
                if (s2 != null && s2.getMetaInfo().pset != null) {
                    final PartitionedSourcePosition sp2 = (appCheckpoint != null) ? appCheckpoint.getPartitionedSourcePositionForComponent(s2.getMetaID()) : null;
                    if (Logger.getLogger("Recovery").isDebugEnabled()) {
                        Logger.getLogger("Recovery").debug((Object)("Starting persistent stream " + s2.getMetaName() + " at position " + sp2));
                    }
                    s2.setPositionAndStartEmitting(sp2);
                }
            }
            for (final IStreamGenerator g : this.gens) {
                g.start();
            }
            for (final Flow s3 : this.flows) {
                s3.startSources(servers);
            }
        }
        catch (Exception e) {
            this.stopAllComponents();
            this.running = false;
            throw e;
        }
    }
    
    private void stopStreamsAndRecordCheckpoint() {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot stop streams for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        this.running = false;
        for (final Stream stream : this.usedStreams.values()) {
            if (stream != null) {
                if (stream.getMetaInfo().pset != null) {
                    stream.stop();
                }
                else {
                    final UUID streamUUID = stream.getMetaID();
                    if (this.flowInfo == null || this.flowInfo.objects == null || !this.flowInfo.objects.containsKey(EntityType.STREAM) || !this.flowInfo.objects.get(EntityType.STREAM).contains(streamUUID)) {
                        continue;
                    }
                    stream.stop();
                }
            }
        }
        for (final Flow s : this.flows) {
            s.stopStreamsAndRecordCheckpoint();
        }
    }
    
    @Override
    public synchronized void stop() throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot stop flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        this.stopStreamsAndRecordCheckpoint();
        this.stopAllComponents();
    }
    
    @Override
    public void recordLagMarker(final LagMarker lagMarker) {
    }
    
    private void stopAllComponents() throws Exception {
        final List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = this.getFlowMeta().showTopology();
        for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo) {
            final Flow f = (Flow)this.srv.getOpenObject(pair.second.getUuid());
            if (f != null) {
                try {
                    f.stopObjects(pair.first);
                }
                catch (Exception e) {
                    Flow.logger.warn((Object)("Internal error while stopping flow - " + f.getMetaFullName() + " Reason - " + e.getLocalizedMessage()));
                }
            }
        }
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    private void undeploy(final List<? extends FlowComponent> list) throws Exception {
        for (final FlowComponent o : list) {
            this.srv.removeDeployedObject(o);
            o.setFlow(null);
        }
    }
    
    private void undeploy(final Collection<UUID> list) throws Exception {
        for (final UUID id : list) {
            this.srv.removeDeployedObjectById(id);
        }
    }
    
    @Override
    public synchronized void close() throws Exception {
        this.stop();
        if (this.nodeManager != null) {
            this.nodeManager.close();
            this.nodeManager = null;
        }
        this.undeploy(this.flows);
        this.undeploy(this.gens);
        this.undeploy(this.sources);
        this.undeploy(this.targets);
        this.undeploy(this.cqs);
        this.undeploy(this.windows);
        this.undeploy(this.sorters);
        this.undeploy(this.caches);
        this.undeploy(this.wastores);
        this.undeploy(this.wastoreviews);
        this.undeploy(this.multiComponents);
        for (final EntityType t : new EntityType[] { EntityType.FLOW, EntityType.STREAM_GENERATOR, EntityType.SOURCE, EntityType.TARGET, EntityType.CQ, EntityType.WINDOW, EntityType.SORTER, EntityType.CACHE, EntityType.HDSTORE, EntityType.WASTOREVIEW }) {
            this.undeploy(this.flowInfo.getObjects(t));
        }
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        for (final Stream stream : this.usedStreams.values()) {
            if (stream != null && stream.getChannel() instanceof KafkaChannel) {
                if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                    final Map m = ((KafkaChannel)stream.getChannel()).getStats();
                    if (m != null) {
                        System.out.println("Kafka Stats for " + stream.getMetaFullName() + " : " + m);
                    }
                }
                final Map m = ((KafkaChannel)stream.getChannel()).getStats();
                ((KafkaChannel)stream.getChannel()).addSpecificMonitorEvents(monEvs);
            }
        }
    }
    
    public Collection<MonitorEvent> getStreamMonEvs(final long ts) {
        final Collection<MonitorEvent> result = new ArrayList<MonitorEvent>();
        for (final Stream stream : this.usedStreams.values()) {
            if (stream != null) {
                final Collection<MonitorEvent> streamMonEvents = stream.getMonitorEvents(ts);
                result.addAll(streamMonEvents);
            }
        }
        return result;
    }
    
    public static CopyOnWriteArrayList<UUID> getLatestVersions(final Set<UUID> metaObjectOfSomeType, final EntityType entityType) throws Exception {
        final Map<String, MetaInfo.MetaObject> latestVersions = Collections.synchronizedMap(new LinkedHashMap<String, MetaInfo.MetaObject>());
        for (final UUID id : metaObjectOfSomeType) {
            final MetaInfo.MetaObject o = Server.server.getObjectInfo(id, entityType);
            if (latestVersions.containsKey(o.name) && latestVersions.get(o.name).version < o.version) {
                latestVersions.put(o.name, o);
            }
            else {
                if (latestVersions.containsKey(o.name)) {
                    continue;
                }
                latestVersions.put(o.name, o);
            }
        }
        final CopyOnWriteArrayList<UUID> result = new CopyOnWriteArrayList<UUID>();
        for (final MetaInfo.MetaObject latesMetaObjects : latestVersions.values()) {
            result.add(latesMetaObjects.uuid);
        }
        if (Flow.logger.isTraceEnabled()) {
            for (final Map.Entry<String, MetaInfo.MetaObject> obj : latestVersions.entrySet()) {
                Flow.logger.trace((Object)(entityType + " " + obj.getKey() + " " + obj.getValue().version));
            }
        }
        return result;
    }
    
    private void connectParts() throws Exception {
        for (final Compound c : this.compoundComponents) {
            c.connectParts(this);
        }
        for (final Flow f : this.flows) {
            f.connectParts();
        }
    }
    
    public boolean deploySubFlows(final List<UUID> actionParam) throws Exception {
        assert this.flowInfo.deploymentPlan != null;
        final boolean deployed = false;
        for (final UUID id : getLatestVersions(this.flowInfo.getObjects(EntityType.FLOW), EntityType.FLOW)) {
            if (actionParam.contains(id)) {
                final MetaInfo.Flow o = Server.server.getObjectInfo(id, EntityType.FLOW);
                Server.server.checkNotDeployed(o);
                final Flow flow = Server.server.createFlow(o, this);
                this.flows.add(flow);
            }
        }
        this.deployedFlows.addAll(actionParam);
        return deployed;
    }
    
    public boolean deployAdhocSubFlows() throws Exception {
        assert this.parent.flowInfo.deploymentPlan != null;
        final boolean deployed = false;
        for (final UUID id : getLatestVersions(this.flowInfo.getObjects(EntityType.FLOW), EntityType.FLOW)) {
            final MetaInfo.Flow o = Server.server.getObjectInfo(id, EntityType.FLOW);
            final boolean canFlowBeDeployed = DeployUtility.canFlowBeDeployed(o, Server.server.getServerID(), this.flowInfo);
            if (canFlowBeDeployed) {
                Server.server.checkNotDeployed(o);
                final Flow flow = Server.server.createFlow(o, this);
                this.deployedFlows.add(this.getMetaID());
                this.flows.add(flow);
            }
        }
        return deployed;
    }
    
    public boolean checkStrategy(final MetaInfo.Flow.Detail d, final MetaInfo.DeploymentGroup dg) throws Exception {
        if (d.strategy == DeploymentStrategy.ON_ALL) {
            return true;
        }
        final Long val = dg.groupMembers.get(this.srv.getServerID());
        if (val == null) {
            throw new RuntimeException("server is not in <" + dg.name + "> deployment group");
        }
        final long idInGroup = val;
        for (final long id : dg.groupMembers.values()) {
            if (idInGroup > id) {
                Flow.logger.info((Object)("Flow " + d.flow + " cannot be deployed because the server is not in lowest id in the deployment group"));
                return false;
            }
        }
        return true;
    }
    
    public void deploy(final List<UUID> actionParam) throws Exception {
        this.deployObjects(actionParam);
        this.connectParts();
    }
    
    public void deployAdhocFlow() throws Exception {
        final boolean canFlowBeDeployed = DeployUtility.canFlowBeDeployed(this.flowInfo, Server.server.getServerID(), null);
        if (canFlowBeDeployed) {
            this.deployedFlows.add(this.getMetaID());
        }
        this.deployObjects(null);
        this.connectParts();
    }
    
    void deployObjects(final List<UUID> flowsTobeDeployed) throws Exception {
        final List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = this.getFlowMeta().showTopology();
        for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo) {
            final Flow flow = (Flow)this.srv.getOpenObject(pair.second.getUuid());
            if (flow != null && this.deployedFlows.contains(flow.getMetaID()) && (flowsTobeDeployed == null || flowsTobeDeployed.contains(flow.getMetaID()))) {
                if (flow.getMetaInfo().getType() == EntityType.FLOW) {
                    if (Flow.logger.isDebugEnabled()) {
                        Flow.logger.debug((Object)("for flow :" + flow.getMetaFullName() + ", setting :" + this.getMetaFullName() + " as parent flow."));
                    }
                    flow.setFlow(this);
                }
                final long startTime = System.currentTimeMillis();
                flow.deployObject(pair.first);
                final long deployTime = System.currentTimeMillis() - startTime;
                if (!Flow.logger.isInfoEnabled()) {
                    continue;
                }
                Flow.logger.info((Object)("Time taken to deploy component " + pair.first.getFullName() + " is " + deployTime + " ms"));
            }
        }
    }
    
    void deployObject(final MetaInfo.MetaObject o) throws Exception {
        if (o.getType().equals(EntityType.TARGET)) {
            final MetaInfo.Target target = (MetaInfo.Target)o;
            final Map parallelismProperties = target.getParallelismProperties();
            if (parallelismProperties != null) {
                final MultiComponent mc = this.srv.createMultiComponent(o);
                final Integer parallelismFactor = Integer.parseInt(parallelismProperties.get("parallelismFactor").toString());
                mc.setNumInstances(parallelismFactor);
                for (int i = 0; i < parallelismFactor; ++i) {
                    final Target tg = this.srv.createTarget(target, false);
                    tg.setFlow(this);
                    mc.addComponent(tg);
                }
                mc.setFlow(this);
                this.multiComponents.add(mc);
                this.compoundComponents.add(mc);
                return;
            }
        }
        if (o.getType().equals(EntityType.STREAM)) {
            this.createStream(o.getUuid(), this);
            return;
        }
        this.srv.checkNotDeployed(o);
        final FlowComponent w = this.srv.createComponent(o);
        if (w == null) {
            return;
        }
        w.setFlow(this);
        if (Flow.logger.isDebugEnabled()) {
            Flow.logger.debug((Object)("for component :" + w.getMetaFullName() + ", setting :" + this.getMetaFullName() + " as parent flow."));
        }
        switch (o.getType()) {
            case CACHE: {
                this.caches.add((Cache)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case CQ: {
                this.cqs.add((CQTask)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case SORTER: {
                this.sorters.add((Sorter)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case SOURCE: {
                this.sources.add((Source)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case STREAM_GENERATOR: {
                this.gens.add((IStreamGenerator)w);
                break;
            }
            case TARGET: {
                this.targets.add((Target)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case HDSTORE: {
                this.wastores.add((HStore)w);
                break;
            }
            case WASTOREVIEW: {
                this.wastoreviews.add((HStoreView)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
            case WINDOW: {
                this.windows.add((IWindow)w);
                this.compoundComponents.add((Compound)w);
                break;
            }
        }
    }
    
    public List<FlowComponent> getDeployedObjects() {
        final List<FlowComponent> list = new ArrayList<FlowComponent>();
        list.add(this);
        list.addAll(this.sources);
        list.addAll(this.gens);
        list.addAll(this.targets);
        list.addAll(this.cqs);
        list.addAll(this.windows);
        list.addAll(this.caches);
        list.addAll(this.wastores);
        list.addAll(this.multiComponents);
        for (final UUID entry : this.usedStreams.keySet()) {
            Stream s = null;
            try {
                s = this.getStream(entry);
            }
            catch (Exception ex) {}
            if (s != null) {
                list.add(s);
            }
        }
        for (final Flow f : this.flows) {
            list.addAll(f.getDeployedObjects());
        }
        return list;
    }
    
    public static List<MetaInfo.MetaObjectInfo> getObjectsMeta(final List<FlowComponent> objs) {
        final List<MetaInfo.MetaObjectInfo> list = new ArrayList<MetaInfo.MetaObjectInfo>();
        for (final FlowComponent o : objs) {
            list.add(o.getMetaObjectInfo());
        }
        return list;
    }
    
    public List<FlowComponent> initCaches() throws Exception {
        final List<FlowComponent> list = new ArrayList<FlowComponent>();
        for (final Cache c : this.caches) {
            c.registerAndInitCache();
        }
        list.addAll(this.caches);
        for (final Flow f : this.flows) {
            final List<FlowComponent> l = f.initCaches();
            list.addAll(l);
        }
        return list;
    }
    
    public void startAllCaches(final List<UUID> servers) throws Exception {
        final List<UUID> relevantServers = findServersWithThisFlow(servers, this.flowInfo.getUuid());
        for (final HStore ws : this.wastores) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Starting cache for hd store " + ws.getMetaName() + " with servers " + relevantServers));
            }
            ws.startCache(relevantServers);
        }
        for (final Cache c : this.caches) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Starting cache " + c.getMetaName() + " with servers " + relevantServers));
            }
            c.startCache(relevantServers);
        }
        for (final Flow f : this.flows) {
            f.startAllCaches(servers);
        }
    }
    
    public static List<UUID> findServersWithThisFlow(final List<UUID> servers, final UUID flowUuid) throws MetaDataRepositoryException {
        final Collection<UUID> wd = MetadataRepository.getINSTANCE().getServersForDeployment(flowUuid, HSecurityManager.TOKEN);
        final List<UUID> whereDeployed = new ArrayList<UUID>();
        for (final Object obj : wd) {
            if (obj instanceof UUID) {
                if (!servers.contains(obj)) {
                    continue;
                }
                whereDeployed.add((UUID)obj);
            }
            else {
                if (!(obj instanceof Collection)) {
                    continue;
                }
                final Collection<UUID> uuids = (Collection<UUID>)obj;
                for (final UUID uuid : uuids) {
                    if (servers.contains(uuid)) {
                        whereDeployed.add(uuid);
                    }
                }
            }
        }
        return whereDeployed;
    }
    
    private List<Source> getAllSources() {
        final List<Source> result = new ArrayList<Source>();
        result.addAll(this.sources);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllSources());
        }
        return result;
    }
    
    private List<CQTask> getAllCQs() {
        final List<CQTask> result = new ArrayList<CQTask>();
        result.addAll(this.cqs);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllCQs());
        }
        return result;
    }
    
    private Set<Stream> getAllStreams() {
        final Set<Stream> result = new HashSet<Stream>();
        result.addAll(this.usedStreams.values());
        for (final Flow f : this.flows) {
            result.addAll(f.getAllStreams());
        }
        return result;
    }
    
    public List<HStore> getAllHDStores() {
        final List<HStore> result = new ArrayList<HStore>();
        result.addAll(this.wastores);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllHDStores());
        }
        return result;
    }
    
    private List<Target> getAllTargets() {
        final List<Target> result = new ArrayList<Target>();
        result.addAll(this.targets);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllTargets());
        }
        return result;
    }
    
    private List<IWindow> getAllWindows() {
        final List<IWindow> result = new ArrayList<IWindow>();
        result.addAll(this.windows);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllWindows());
        }
        return result;
    }
    
    private List<MultiComponent> getAllMultiComponents() {
        final List<MultiComponent> result = new ArrayList<MultiComponent>();
        result.addAll(this.multiComponents);
        for (final Flow f : this.flows) {
            result.addAll(f.getAllMultiComponents());
        }
        return result;
    }
    
    public Stream getStream(final UUID streamID) {
        final Stream s = this.usedStreams.get(streamID);
        return s;
    }
    
    private Stream createStream(final UUID uuid, final Flow flow) throws Exception {
        Stream stream_runtime = this.usedStreams.get(uuid);
        if (stream_runtime == null) {
            final MetaInfo.Stream stream_metaobject = (MetaInfo.Stream)this.mdRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
            if (stream_metaobject.pset != null) {
                stream_runtime = new Stream(stream_metaobject, Server.getServer(), flow);
                if (Flow.logger.isDebugEnabled()) {
                    Flow.logger.debug((Object)("Created new kafka stream runtime object for : " + stream_metaobject.getFullName() + " to be used in flow : " + flow.getMetaFullName()));
                }
            }
            else {
                stream_runtime = new Stream(stream_metaobject, Server.getServer(), flow);
            }
            this.usedStreams.put(uuid, stream_runtime);
        }
        else {
            Flow.logger.warn((Object)"Unexpected state: Stream was not NULL");
        }
        return stream_runtime;
    }
    
    public HStore getHDStore(final UUID wasID) throws Exception {
        for (final HStore was : this.wastores) {
            if (was.getMetaID().equals((Object)wasID)) {
                return was;
            }
        }
        final HStore was2 = (HStore)this.srv.getOpenObject(wasID);
        if (was2 != null) {
            return was2;
        }
        final MetaInfo.HDStore store = this.srv.getObjectInfo(wasID, EntityType.HDSTORE);
        if (this.getMetaInfo().getMetaInfoStatus().isAdhoc()) {
            throw new RuntimeException("HDStore [" + store.getFullName() + "] is not deployed, so query fails.");
        }
        throw new RuntimeException("cannot connect to not deployed HDStore [" + store.getFullName() + "]");
    }
    
    private FlowComponent getPubSub(final UUID id) throws Exception {
        if (this.usedStreams.containsKey(id)) {
            Stream s = this.getStream(id);
            if (s == null) {
                s = this.createStream(id, this);
                if (Flow.logger.isInfoEnabled()) {
                    Flow.logger.info((Object)("Getting access to a stream in a different flow : " + s.getMetaFullName()));
                }
            }
            else if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Getting access to a stream in the current flow : " + s.getMetaFullName()));
            }
            s.connectedFlow(this);
            return s;
        }
        final FlowComponent o = this.srv.getOpenObject(id);
        if (o != null) {
            assert !(o instanceof Stream);
            return o;
        }
        else {
            final MetaInfo.MetaObject obj = this.srv.getObject(id);
            if (obj == null) {
                throw new ServerException(this.getMetaType() + " " + this.getMetaName() + " tries to access unknown object " + id);
            }
            throw new ServerException(this.getMetaType() + " " + this.getMetaName() + " tries to access undeployed object " + obj.type + " " + obj.name + " " + obj.uuid);
        }
    }
    
    public Subscriber getSubscriber(final UUID subID) throws Exception {
        return (Subscriber)this.getPubSub(subID);
    }
    
    public Publisher getPublisher(final UUID pubID) throws Exception {
        return (Publisher)this.getPubSub(pubID);
    }
    
    public List<Flow> getAllFlows() {
        if (this.flows.isEmpty()) {
            return Collections.singletonList(this);
        }
        final List<Flow> ret = new ArrayList<Flow>();
        ret.add(this);
        for (final Flow f : this.flows) {
            ret.addAll(f.getAllFlows());
        }
        return ret;
    }
    
    public SourcePosition getRestartSourcePositionForSourceUUID(final UUID sourceUUID) {
        SourcePosition result = null;
        final Position p = Utility.updateUuids(StatusDataStore.getInstance().getAppCheckpoint(this.getMetaID()));
        if (p != null) {
            result = p.getLowSourcePositionForComponent(sourceUUID);
        }
        return result;
    }
    
    @Override
    public boolean recoveryIsEnabled() {
        final Integer r = this.getRecoveryType();
        return r == 2;
    }
    
    public Long getRecoveryPeriod() {
        final long period = (this.recoveryPeriod != null) ? this.recoveryPeriod : this.flowInfo.recoveryPeriod;
        return period;
    }
    
    public void setRecoveryPeriod(final Long recoveryPeriod) {
        this.recoveryPeriod = recoveryPeriod;
        for (final Flow s : this.flows) {
            s.setRecoveryPeriod(recoveryPeriod);
        }
    }
    
    public Integer getRecoveryType() {
        if (this.parent != null) {
            return this.parent.getRecoveryType();
        }
        final int type = (this.recoveryType != null) ? this.recoveryType : this.flowInfo.recoveryType;
        return type;
    }
    
    public void setRecoveryType(final Integer recoveryType) {
        this.recoveryType = recoveryType;
        for (final Flow s : this.flows) {
            s.setRecoveryType(recoveryType);
        }
    }
    
    private PathManager getEndpointCheckpoint() {
        if (Flow.logger.isDebugEnabled()) {
            Flow.logger.debug((Object)"Trying to build position from HDStore checkpoints...");
        }
        PathManager result = new PathManager();
        final List<HStore> allHDStores = this.getAllHDStores();
        for (final HStore ws : allHDStores) {
            final Position wsCheckpoint = ws.getPersistedCheckpointPosition();
            result.mergeLowerPositions(wsCheckpoint);
        }
        final List<Target> allTargets = this.getAllTargets();
        for (final Target t : allTargets) {
            final Position tCheckpoint = t.getEndpointCheckpoint();
            result.mergeLowerPositions(tCheckpoint);
        }
        final List<MultiComponent> allMultiComponents = this.getAllMultiComponents();
        for (final MultiComponent mc : allMultiComponents) {
            if (mc.getMetaType().equals(EntityType.TARGET)) {
                final Position mCheckpoint = mc.getEndpointCheckpoint();
                result.mergeLowerPositions(mCheckpoint);
            }
        }
        if (result.isEmpty()) {
            return null;
        }
        result = Utility.updateUuids(result);
        return result;
    }
    
    public void bindParams(final List<Property> params) {
        for (final CQTask cq : this.cqs) {
            if (!cq.bindParameters(params)) {
                throw new RuntimeException("some parameters are missing for app <" + this.getMetaName() + ">");
            }
        }
    }
    
    public MetaInfo.Flow getFlowMeta() {
        return this.flowInfo;
    }
    
    public String getDistributionId() {
        String ret = BaseServer.getServerName();
        final MetaInfo.Flow.Detail detail = DeployUtility.haveDeploymentDetail(this.flowInfo, (this.parent == null) ? null : this.parent.flowInfo);
        if (detail.strategy == DeploymentStrategy.ON_ONE) {
            ret = null;
        }
        return ret;
    }
    
    @Override
    public void startFlow(final List<UUID> servers, final Long epochNumber) throws Exception {
        if (epochNumber != null) {
            this.epochNumber = epochNumber;
        }
        this.start(servers);
        if (this.nodeManager != null) {
            this.nodeManager.flowStarted(epochNumber);
        }
    }
    
    @Override
    public void startFlow(final Long epochNumber) throws Exception {
        this.start();
        if (this.nodeManager != null) {
            this.nodeManager.flowStarted(epochNumber);
        }
    }
    
    @Override
    public void stopFlow(final Long epochNumber) throws Exception {
        this.stop();
        if (this.nodeManager != null) {
            this.nodeManager.flowStopped(epochNumber);
        }
    }
    
    @Override
    public void pauseSources(final Long epochNumber) throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot pause sources for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        final Set<MetaInfo.MetaObject> semanticSources = this.getFlowMeta().getSemanticSources();
        for (final MetaInfo.MetaObject mo : semanticSources) {
            final FlowComponent component = this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                Flow.logger.warn((Object)("Unable to find component " + mo.getName() + " to pause sources for quiesce on " + BaseServer.getServerName()));
            }
            else {
                component.setPaused(true);
            }
        }
    }
    
    public void quiesceFlow(final Long commandTimestamp) throws Exception {
        if (this.nodeManager != null) {
            this.nodeManager.notifyNodeProcessedCommand(Event.EventAction.API_QUIESCE, commandTimestamp);
        }
    }
    
    @Override
    public void approveQuiesce(final Long epochNumber) throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot ask sources to approve quiesce for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        final Set<MetaInfo.MetaObject> semanticSources = this.getFlowMeta().getSemanticSources();
        for (final MetaInfo.MetaObject mo : semanticSources) {
            if (mo.getType() != EntityType.SOURCE && mo.getType() != EntityType.STREAM) {
                Flow.logger.info((Object)("Found unusual, but allowable, component as starting point: " + mo.getName() + " is a " + mo.getType()));
            }
            final FlowComponent component = this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                Flow.logger.warn((Object)("Unable to find component " + mo.getName() + " to approve quiesce on " + BaseServer.getServerName()));
            }
            else {
                if (!component.approveQuiesce()) {
                    throw new DisapproveQuiesceException("Source disapproved quiesce: " + component.getMetaName(), component.getMetaName());
                }
                continue;
            }
        }
    }
    
    @Override
    public void unpauseSources(final Long epochNumber) throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot unpause sources for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        final Set<MetaInfo.MetaObject> semanticSources = this.getFlowMeta().getSemanticSources();
        for (final MetaInfo.MetaObject mo : semanticSources) {
            final FlowComponent component = this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                Flow.logger.warn((Object)("Unable to find component " + mo.getName() + " to quiesce on " + BaseServer.getServerName()));
            }
            else {
                component.setPaused(false);
            }
        }
    }
    
    @Override
    public void checkpoint(final Long commandTimestamp) throws Exception {
        if (!this.running) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)("Cannot take checkpoint for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Taking checkpoint for flow " + this.getMetaName() + " at time " + commandTimestamp));
        }
        final Set<UUID> deployedComponents = new HashSet<UUID>();
        for (final FlowComponent c : this.getDeployedObjects()) {
            if (c.getMetaType() != EntityType.FLOW && c.getMetaType() != EntityType.APPLICATION) {
                if (c instanceof Stream) {
                    continue;
                }
                deployedComponents.add(c.getMetaID());
            }
        }
        if (deployedComponents.isEmpty()) {
            this.nodeManager.notifyNodeProcessedCommand(Event.EventAction.NODE_APP_CHECKPOINTED, commandTimestamp);
            return;
        }
        this.nodeManager.waitForCommand(commandTimestamp, deployedComponents, Event.EventAction.NODE_APP_CHECKPOINTED);
        final Set<MetaInfo.MetaObject> startingPoints = this.getFlowMeta().getStartingPoints();
        final CheckpointCommandEvent commandEvent = new CheckpointCommandEvent(this.getTopLevelFlow().getMetaID(), commandTimestamp);
        for (final MetaInfo.MetaObject mo : startingPoints) {
            final FlowComponent component = (mo.getType() == EntityType.STREAM) ? ((Stream)this.getUsedStreams().get(mo.getUuid())) : this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                continue;
            }
            component.injectCommandEvent(commandEvent);
        }
    }
    
    @Override
    public void quiesceCheckpoint(final Long commandTimestamp) throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot take checkpoint for flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        final Set<UUID> deployedComponents = new HashSet<UUID>();
        for (final FlowComponent c : this.getDeployedObjects()) {
            if (c.getMetaType() != EntityType.FLOW && c.getMetaType() != EntityType.APPLICATION) {
                if (c instanceof Stream) {
                    continue;
                }
                deployedComponents.add(c.getMetaID());
            }
        }
        if (deployedComponents.isEmpty()) {
            this.nodeManager.notifyNodeProcessedCommand(Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED, commandTimestamp);
            return;
        }
        this.nodeManager.waitForCommand(commandTimestamp, deployedComponents, Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED);
        final Set<MetaInfo.MetaObject> startingPoints = this.getFlowMeta().getStartingPoints();
        final QuiesceCheckpointCommandEvent commandEvent = new QuiesceCheckpointCommandEvent(this.getTopLevelFlow().getMetaID(), commandTimestamp);
        for (final MetaInfo.MetaObject mo : startingPoints) {
            final FlowComponent component = (mo.getType() == EntityType.STREAM) ? ((Stream)this.getUsedStreams().get(mo.getUuid())) : this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                Flow.logger.warn((Object)("Unable to find component " + mo.getName() + " to quiesce on " + BaseServer.getServerName()));
            }
            else {
                component.injectCommandEvent(commandEvent);
            }
        }
    }
    
    @Override
    public void quiesceFlush(final Long commandTimestamp) throws Exception {
        if (!this.running) {
            if (Flow.logger.isInfoEnabled()) {
                Flow.logger.info((Object)("Cannot flush flow " + this.getMetaName() + " because it is not running"));
            }
            return;
        }
        final Set<MetaInfo.MetaObject> startingPoints = this.getFlowMeta().getStartingPoints();
        final Set<UUID> deployedComponents = new HashSet<UUID>();
        for (final FlowComponent c : this.getDeployedObjects()) {
            if (c.getMetaType() != EntityType.FLOW && c.getMetaType() != EntityType.APPLICATION) {
                if (c instanceof Stream) {
                    continue;
                }
                deployedComponents.add(c.getMetaID());
            }
        }
        if (deployedComponents.isEmpty()) {
            this.nodeManager.notifyNodeProcessedCommand(Event.EventAction.NODE_APP_QUIESCE_FLUSHED, commandTimestamp);
            return;
        }
        this.nodeManager.waitForCommand(commandTimestamp, deployedComponents, Event.EventAction.NODE_APP_QUIESCE_FLUSHED);
        final FlushCommandEvent commandEvent = new FlushCommandEvent(this.getTopLevelFlow().getMetaID(), commandTimestamp);
        for (final MetaInfo.MetaObject mo : startingPoints) {
            final FlowComponent component = (mo.getType() == EntityType.STREAM) ? ((Stream)this.getUsedStreams().get(mo.getUuid())) : this.srv.getOpenObject(mo.getUuid());
            if (component == null) {
                Flow.logger.warn((Object)("Unable to find component " + mo.getName() + " to quiesce on " + BaseServer.getServerName()));
            }
            else {
                component.injectCommandEvent(commandEvent);
            }
        }
    }
    
    @Override
    public void memoryStatus() throws Exception {
        Flow.logger.warn((Object)"MEMORY STATUS IS NOT IMPLEMENTED");
    }
    
    @Override
    public void startCaches(final List<UUID> servers, final Long epochNumber) throws Exception {
        this.startAllCaches(servers);
        if (this.nodeManager != null) {
            this.nodeManager.flowDeployed(epochNumber);
        }
    }
    
    @Override
    public void stopCaches(final Long epochNumber) throws Exception {
        for (final HStore ws : this.wastores) {
            ws.stopCache();
        }
        for (final Cache c : this.caches) {
            c.stopCache();
        }
        for (final Flow f : this.flows) {
            f.stopCaches(epochNumber);
        }
    }
    
    public Map<UUID, Stream> getUsedStreams() {
        return this.usedStreams;
    }
    
    private Set<UUID> getDeployedComponents() {
        final Set<UUID> result = new HashSet<UUID>();
        for (final FlowComponent c : this.getDeployedObjects()) {
            if (c.getMetaType() != EntityType.FLOW && c.getMetaType() != EntityType.APPLICATION) {
                result.add(c.getMetaID());
            }
        }
        return result;
    }
    
    public int getInDegrees(final UUID targetUuid) {
        if (this.indegrees == null) {
            this.indegrees = this.getFlowMeta().getInDegrees();
        }
        try {
            if (Flow.logger.isDebugEnabled()) {
                final Object o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(targetUuid, HSecurityManager.TOKEN);
                Flow.logger.debug((Object)("Getting in degrees for " + o));
            }
        }
        catch (MetaDataRepositoryException e) {
            Flow.logger.warn((Object)e.getMessage());
        }
        final Integer ins = this.indegrees.get(targetUuid);
        return (ins == null) ? 1 : ins;
    }
    
    static {
        Flow.logger = Logger.getLogger((Class)Flow.class);
    }
}
