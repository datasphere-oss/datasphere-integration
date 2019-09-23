package com.datasphere.metaRepository;

import org.apache.log4j.*;

import com.datasphere.runtime.meta.*;
import com.datasphere.intf.*;
import com.hazelcast.core.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.utility.*;
import org.eclipse.persistence.exceptions.*;
import javax.persistence.*;
import com.datasphere.runtime.*;
import com.datasphere.security.*;
import java.util.*;
import com.datasphere.persistence.*;
import com.datasphere.recovery.*;

public class StatusDataStore
{
    private static Logger logger;
    private static StatusDataStore instance;
    private volatile PersistenceLayer theDb;
    
    public StatusDataStore() {
        this.theDb = null;
    }
    
    public static StatusDataStore getInstance() {
        if (StatusDataStore.instance == null) {
            synchronized (StatusDataStore.class) {
                if (StatusDataStore.instance == null) {
                    StatusDataStore.instance = new StatusDataStore();
                }
            }
        }
        return StatusDataStore.instance;
    }
    
    private PersistenceLayer getDb() {
        if (this.theDb == null) {
            synchronized (this) {
                if (this.theDb == null) {
                    if (Server.persistenceIsEnabled()) {
                        try {
                            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                                Logger.getLogger("Recovery").debug((Object)"Attempting to get DB connection for ...");
                            }
                            final IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
                            final String clusterName = HazelcastSingleton.getClusterName();
                            final MetaInfo.Initializer initializer = (MetaInfo.Initializer)startUpMap.get((Object)clusterName);
                            final String DBUname = initializer.MetaDataRepositoryUname;
                            final String DBPassword = initializer.MetaDataRepositoryPass;
                            final String DBName = initializer.MetaDataRepositoryDBname;
                            final String DBLocation = initializer.MetaDataRepositoryLocation;
                            final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
                            final Map<String, Object> props = new HashMap<String, Object>();
                            if (DBUname != null && !DBUname.isEmpty()) {
                                props.put("javax.persistence.jdbc.user", DBUname);
                            }
                            if (DBPassword != null && !DBPassword.isEmpty()) {
                                props.put("javax.persistence.jdbc.password", DBPassword);
                            }
                            props.put("eclipselink.jdbc.batch-writing", "JDBC");
                            props.put("eclipselink.jdbc.batch-writing.size", "1000");
                            if (DBName != null && !DBName.isEmpty() && DBUname != null && !DBUname.isEmpty() && DBPassword != null && !DBPassword.isEmpty() && DBLocation != null && !DBLocation.isEmpty()) {
                                props.put("javax.persistence.jdbc.url", metaDataDbProvider.getJDBCURL(DBLocation, DBName, DBUname, DBPassword));
                            }
                            final PersistenceLayer pl = PersistenceFactory.createPersistenceLayerWithRetry(null, PersistenceFactory.PersistingPurpose.RECOVERY, "status-data-cache", props);
                            pl.init();
                            this.theDb = pl;
                        }
                        catch (PersistenceUnitLoadingException e) {
                            StatusDataStore.logger.error((Object)"Error initializing the HD App Checkpoint persistence unit, which indicates a bad configuration", (Throwable)e);
                        }
                        catch (Exception e2) {
                            StatusDataStore.logger.error((Object)("Unexpected error when building database connection: " + e2.getMessage()), (Throwable)e2);
                        }
                    }
                    else if (Logger.getLogger("Recovery").isInfoEnabled()) {
                        Logger.getLogger("Recovery").info((Object)"HD persistence is OFF; turn it on by specifying com.datasphere.config.persist=True");
                    }
                }
            }
        }
        return this.theDb;
    }
    
    public synchronized boolean putAppCheckpoint(final UUID flowUuid, final Position checkpointPosition) {
        if (flowUuid == null) {
            StatusDataStore.logger.warn((Object)"Could not write source restart position because the UUID provided was null");
            return false;
        }
        if (checkpointPosition == null || checkpointPosition.isEmpty()) {
            StatusDataStore.logger.debug((Object)"Could not write source restart position because the Checkpoint provided was null or empty");
            return false;
        }
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                StatusDataStore.logger.warn((Object)"Could not write source restart position because DB connection could not be established");
                return false;
            }
            final List<CheckpointPath> checkpointPaths = new ArrayList<CheckpointPath>();
            for (final Path path : checkpointPosition.values()) {
                checkpointPaths.add(new CheckpointPath(flowUuid, path.getPathItems(), path.getLowSourcePosition(), path.getHighSourcePosition(), path.getAtOrAfter()));
            }
            final Object persistResult = db.persist(checkpointPaths);
            if (persistResult == null) {
                Logger.getLogger("Recovery").warn((Object)"Sent checkpoint paths to persistence layer but the write failed");
                return false;
            }
            this.summarizeAppCheckpoint(flowUuid, checkpointPosition, 0L);
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug((Object)"Successfully Wrote App Checkpoint:");
                Utility.prettyPrint(checkpointPosition);
            }
            return true;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return false;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return false;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return false;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()), (Throwable)e4);
            return false;
        }
    }
    
    public synchronized boolean putPendingAppCheckpoint(final UUID flowUuid, final Position checkpointPosition, final long commandTimestamp) {
        if (flowUuid == null) {
            StatusDataStore.logger.warn((Object)"Could not write app checkpoint portion because the UUID provided was null");
            return false;
        }
        if (checkpointPosition == null || checkpointPosition.isEmpty()) {
            StatusDataStore.logger.debug((Object)"Could not write app checkpoint portion because the Checkpoint provided was null or empty");
            return false;
        }
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                StatusDataStore.logger.warn((Object)"Could not write app checkpoint portion because DB connection could not be established");
                return false;
            }
            final List<PendingCheckpointPath> checkpointPaths = new ArrayList<PendingCheckpointPath>();
            for (final Path path : checkpointPosition.values()) {
                checkpointPaths.add(new PendingCheckpointPath(flowUuid, path.getPathItems(), path.getLowSourcePosition(), path.getHighSourcePosition(), path.getAtOrAfter(), commandTimestamp));
            }
            db.persist(checkpointPaths);
            this.summarizeAppCheckpoint(flowUuid, checkpointPosition, commandTimestamp);
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug((Object)"Successfully Wrote Pending App Checkpoint:");
                Utility.prettyPrint(checkpointPosition);
            }
            return true;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return false;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return false;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return false;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()), (Throwable)e4);
            return false;
        }
    }
    
    private void summarizeAppCheckpoint(final UUID flowUuid, final Position checkpointPosition, final long commandTimestamp) throws Exception {
        final PersistenceLayer db = this.getDb();
        if (db == null) {
            StatusDataStore.logger.warn((Object)"Could not summarize the app checkpoint because DB connection could not be established");
            return;
        }
        final Map<UUID, String> localNameCache = new HashMap<UUID, String>();
        final Map<Pair<String, String>, PendingAppCheckpointSummary> summaries = new HashMap<Pair<String, String>, PendingAppCheckpointSummary>();
        final String flowUri = localNameCache.containsKey(flowUuid) ? localNameCache.get(flowUuid) : MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUuid, HSecurityManager.TOKEN).getUri();
        localNameCache.put(flowUuid, flowUri);
        final String nodeUri = (Server.server != null) ? Server.server.getServerID().toString() : null;
        final long timeStamp = System.currentTimeMillis();
        for (final Path path : checkpointPosition.values()) {
            final UUID sourceUuid = path.getFirstPathItem().getComponentUUID();
            final String sourceDistID = path.getFirstPathItem().getDistributionID();
            final String sourceUri = localNameCache.containsKey(sourceUuid) ? localNameCache.get(sourceUuid) : MetadataRepository.getINSTANCE().getMetaObjectByUUID(sourceUuid, HSecurityManager.TOKEN).getUri();
            localNameCache.put(sourceUuid, sourceUri);
            final UUID componentUuid = path.getLastPathItem().getComponentUUID();
            String componentUri;
            if (localNameCache.containsKey(componentUuid)) {
                componentUri = localNameCache.get(componentUuid);
            }
            else {
                final MetaInfo.MetaObject metaObjectByUUID = MetadataRepository.getINSTANCE().getMetaObjectByUUID(componentUuid, HSecurityManager.TOKEN);
                componentUri = metaObjectByUUID.getUri();
            }
            localNameCache.put(componentUuid, componentUri);
            final Pair<String, String> key = new Pair<String, String>(componentUri, sourceUri);
            PendingAppCheckpointSummary acs = summaries.get(key);
            final SourcePosition lowSourcePosition = path.getLowSourcePosition();
            final SourcePosition highSourcePosition = path.getHighSourcePosition();
            if (acs == null) {
                if (sourceDistID == null) {
                    acs = new PendingAppCheckpointSummary(nodeUri, flowUri, componentUri, sourceUri, lowSourcePosition, highSourcePosition, path.getAtOrAfterBoolean(), commandTimestamp);
                }
                else {
                    final PartitionedSourcePosition partLowSP = new PartitionedSourcePosition();
                    partLowSP.put(sourceDistID, lowSourcePosition);
                    PartitionedSourcePosition partHighSP = null;
                    if (highSourcePosition != null) {
                        partHighSP = new PartitionedSourcePosition();
                        partHighSP.put(sourceDistID, highSourcePosition);
                    }
                    acs = new PendingAppCheckpointSummary(nodeUri, flowUri, componentUri, sourceUri, (SourcePosition)partLowSP, (SourcePosition)partHighSP, path.getAtOrAfterBoolean(), commandTimestamp);
                }
                summaries.put(key, acs);
            }
            else {
                if (acs.lowSourcePosition instanceof PartitionedSourcePosition) {
                    final PartitionedSourcePosition acsPartLowSP = (PartitionedSourcePosition)acs.lowSourcePosition;
                    if (!acsPartLowSP.containsKey(sourceDistID) || acsPartLowSP.compareTo(lowSourcePosition, sourceDistID) > 0) {
                        acsPartLowSP.put(sourceDistID, lowSourcePosition);
                    }
                }
                else if (lowSourcePosition.compareTo(acs.lowSourcePosition) < 0) {
                    acs.setLowSourcePosition(acs.lowSourcePosition);
                }
                if (acs.highSourcePosition instanceof PartitionedSourcePosition) {
                    final PartitionedSourcePosition acsPartHighSP = (PartitionedSourcePosition)acs.highSourcePosition;
                    if (!acsPartHighSP.containsKey(sourceDistID) || acsPartHighSP.compareTo(lowSourcePosition, sourceDistID) < 0) {
                        acsPartHighSP.put(sourceDistID, lowSourcePosition);
                    }
                }
                else if (highSourcePosition != null && highSourcePosition.compareTo(acs.highSourcePosition) > 0) {
                    acs.setHighSourcePosition(highSourcePosition);
                }
                acs.lowSourcePositionText = acs.lowSourcePosition.toHumanReadableString();
            }
        }
        final List<PendingAppCheckpointSummary> persistList = new ArrayList<PendingAppCheckpointSummary>(summaries.values());
        db.persist(persistList);
    }
    
    public synchronized boolean trimAppCheckpoint(final UUID flowUuid, final Position checkpointPosition) {
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                Logger.getLogger("Recovery").warn((Object)"Could not trim source restart position because DB connection could not be established");
                return false;
            }
            final List<CheckpointPath> appCheckpoints = new ArrayList<CheckpointPath>();
            for (final Path path : checkpointPosition.values()) {
                appCheckpoints.add(new CheckpointPath(flowUuid, path.getPathItems(), path.getLowSourcePosition(), path.getHighSourcePosition(), path.getAtOrAfter()));
            }
            final PersistenceLayer pl = db;
            pl.delete(appCheckpoints);
            return true;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to trim app checkpoint because of a communication failure: " + e.getMessage()));
            return false;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to trim app checkpoint: " + e2.getMessage()));
            return false;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to trim app checkpoint: " + e3.getMessage()));
            return false;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to delete from app checkpoint: " + e4.getMessage()));
            return false;
        }
    }
    
    public Position getAppCheckpoint(final UUID appUuid) {
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                StatusDataStore.logger.warn((Object)"Could not get App checkpoint because DB connection could not be established");
                return null;
            }
            final String query = "SELECT c FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid";
            final Map<String, Object> params = new HashMap<String, Object>();
            params.put("flowUuid", appUuid.getUUIDString());
            final List<?> queryResults = db.runQuery(query, params, null);
            if (queryResults == null || queryResults.size() == 0) {
                return null;
            }
            final Set<Path> paths = new HashSet<Path>();
            for (final Object ob : queryResults) {
                if (!(ob instanceof CheckpointPath)) {
                    StatusDataStore.logger.warn((Object)("Unexpected data type when App Checkpoint expected: " + ob.getClass()));
                }
                else {
                    final CheckpointPath c = (CheckpointPath)ob;
                    paths.add(new Path(c.pathItems, c.lowSourcePosition, c.highSourcePosition, c.atOrAfter));
                }
            }
            final Position result = new Position((Set)paths);
            return result;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return null;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return null;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return null;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()));
            return null;
        }
    }
    
    public boolean promotePendingAppCheckpoint(final UUID appUuid, final String appUri, final long commandTimestamp) {
        final PersistenceLayer db = this.getDb();
        if (db == null) {
            StatusDataStore.logger.warn((Object)"Could not promote Pending App Checkpoint because DB connection could not be established");
            return false;
        }
        AppCheckpointPersistenceLayer persistenceLayer;
        if (db instanceof AppCheckpointPersistenceLayer) {
            persistenceLayer = (AppCheckpointPersistenceLayer)db;
        }
        else {
            if (!(db instanceof RetryPersistenceLayer) || !(((RetryPersistenceLayer)db).getPersistenceLayer() instanceof AppCheckpointPersistenceLayer)) {
                StatusDataStore.logger.warn((Object)"Could not promote Pending App Checkpoint because DB connection had an unexpected incompatible type!");
                return false;
            }
            persistenceLayer = (AppCheckpointPersistenceLayer)((RetryPersistenceLayer)db).getPersistenceLayer();
        }
        return persistenceLayer.promotePendingAppCheckpoint(appUuid, appUri, commandTimestamp);
    }
    
    public String getAllAppCheckpointData() {
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                StatusDataStore.logger.warn((Object)"Could not get App checkpoint because DB connection could not be established. This can happen if a Server is running in the cluster. PLEASE MAKE SURE THERE ARE NO SERVERS RUNNING AND TRY AGAIN.");
                return null;
            }
            final String query = "SELECT c FROM AppCheckpoint c";
            final Map<String, Object> params = new HashMap<String, Object>();
            final List<?> queryResults = db.runQuery(query, params, null);
            if (queryResults == null || queryResults.size() == 0) {
                return null;
            }
            final StringBuilder result = new StringBuilder();
            result.append("{\n");
            boolean first = true;
            for (final Object ob : queryResults) {
                if (first) {
                    first = false;
                }
                else {
                    result.append(",");
                }
                if (!(ob instanceof CheckpointPath)) {
                    StatusDataStore.logger.warn((Object)("Unexpected data type when App Checkpoint expected: " + ob.getClass()));
                }
                else {
                    final CheckpointPath c = (CheckpointPath)ob;
                    final Path p = new Path(c.pathItems, c.lowSourcePosition);
                    p.applicationUuid = new UUID(c.flowUuid);
                    result.append(p.toJson()).append("\n");
                }
            }
            result.append("\n}");
            return result.toString();
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return null;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return null;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return null;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()));
            return null;
        }
    }
    
    public synchronized boolean clearAppCheckpoint(final UUID appUuid) {
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                Logger.getLogger("Recovery").warn((Object)"Could not clear App checkpoint because DB connection could not be established");
                return false;
            }
            final String query = "DELETE FROM AppCheckpoint WHERE flowUuid=:flowUuid";
            final Map<String, Object> params = new HashMap<String, Object>();
            params.put("flowUuid", appUuid.getUUIDString());
            final int rowsDeleted = db.executeUpdate(query, params);
            if (Logger.getLogger("Recovery").isInfoEnabled()) {
                Logger.getLogger("Recovery").info((Object)("Cleared the " + rowsDeleted + " rows of the App Checkpoint for UUID=" + appUuid));
            }
            return true;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return false;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return false;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return false;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()));
            return false;
        }
    }
    
    public synchronized boolean clearPendingAppCheckpoint(final UUID appUuid, final Long commandTimestamp) {
        try {
            final PersistenceLayer db = this.getDb();
            if (db == null) {
                Logger.getLogger("Recovery").warn((Object)"Could not clear Pending App Checkpoint because DB connection could not be established");
                return false;
            }
            final String query = "DELETE FROM PendingAppCheckpoint WHERE flowUuid=:flowUuid AND commandTimestamp=:commandTimestamp";
            final Map<String, Object> params = new HashMap<String, Object>();
            params.put("flowUuid", appUuid.getUUIDString());
            params.put("commandTimestamp", commandTimestamp);
            final int rowsDeleted = db.executeUpdate(query, params);
            if (Logger.getLogger("Recovery").isInfoEnabled()) {
                Logger.getLogger("Recovery").info((Object)("Cleared the " + rowsDeleted + " rows of the Pending App Checkpoint for UUID=" + appUuid));
            }
            return true;
        }
        catch (DatabaseException e) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            return false;
        }
        catch (RollbackException e2) {
            StatusDataStore.logger.error((Object)("Rolling back attempt to write status data to table: " + e2.getMessage()));
            return false;
        }
        catch (PersistenceException e3) {
            StatusDataStore.logger.error((Object)("Persistence error during attempt to write status data to table: " + e3.getMessage()));
            return false;
        }
        catch (Exception e4) {
            StatusDataStore.logger.error((Object)("Unable to write status data to table: " + e4.getMessage()));
            return false;
        }
    }
    
    public boolean databaseIsAvailable() {
        final boolean result = this.getDb() != null;
        return result;
    }
    
    public Collection<AppCheckpointSummary> getLowestAppCheckpointSummaries(final UUID componentUuid) {
        final Map<String, AppCheckpointSummary> summaries = new HashMap<String, AppCheckpointSummary>();
        final PersistenceLayer db = this.getDb();
        if (db == null) {
            StatusDataStore.logger.warn((Object)("Could not get summary of app checkpoint for " + componentUuid + " because DB connection could not be established"));
        }
        else {
            try {
                final String componentUri = MetadataRepository.getINSTANCE().getMetaObjectByUUID(componentUuid, HSecurityManager.TOKEN).getUri();
                final String query = "SELECT c FROM AppCheckpointSummary c WHERE c.componentUri=:componentUri OR c.flowUri=:componentUri";
                final Map<String, Object> params = new HashMap<String, Object>();
                params.put("componentUri", componentUri);
                final List<?> queryResults = db.runQuery(query, params, null);
                for (final Object queryResult : queryResults) {
                    assert queryResult instanceof AppCheckpointSummary;
                    final AppCheckpointSummary acs = (AppCheckpointSummary)queryResult;
                    if (acs.lowSourcePosition instanceof PartitionedSourcePosition) {
                        if (summaries.containsKey(acs.sourceUri)) {
                            final AppCheckpointSummary thatAcs = summaries.get(acs.sourceUri);
                            final PartitionedSourcePosition acsPSP = (PartitionedSourcePosition)acs.lowSourcePosition;
                            final Map<String, SourcePosition> acsMap = (Map<String, SourcePosition>)acsPSP.getSourcePositions();
                            final PartitionedSourcePosition thatAcsPSP = (PartitionedSourcePosition)thatAcs.lowSourcePosition;
                            final Map<String, SourcePosition> thatAcsMap = (Map<String, SourcePosition>)thatAcsPSP.getSourcePositions();
                            for (final Map.Entry<String, SourcePosition> entry : acsMap.entrySet()) {
                                if (acsPSP.compareTo((SourcePosition)thatAcsMap.get(entry.getKey()), (String)entry.getKey()) < 0) {
                                    thatAcsPSP.put((String)entry.getKey(), acsPSP.get((String)entry.getKey()));
                                }
                            }
                            summaries.put(acs.sourceUri, thatAcs);
                        }
                        else {
                            summaries.put(acs.sourceUri, acs);
                        }
                    }
                    else if (summaries.containsKey(acs.sourceUri)) {
                        final AppCheckpointSummary thatAcs = summaries.get(acs.sourceUri);
                        if (acs.lowSourcePosition.compareTo(thatAcs.lowSourcePosition) >= 0) {
                            continue;
                        }
                        summaries.put(acs.sourceUri, acs);
                    }
                    else {
                        summaries.put(acs.sourceUri, acs);
                    }
                }
            }
            catch (MetaDataRepositoryException e) {
                StatusDataStore.logger.error((Object)"Could not get app checkpoint summaries", (Throwable)e);
            }
            catch (IllegalArgumentException e2) {
                StatusDataStore.logger.error((Object)"Invalid argument getting app checkpoint summaries", (Throwable)e2);
            }
        }
        final Collection<AppCheckpointSummary> result = summaries.values();
        return result;
    }
    
    static {
        StatusDataStore.logger = Logger.getLogger((Class)StatusDataStore.class);
    }
    
    public static class DropAppCheckpoint implements RemoteCall
    {
        private static final long serialVersionUID = -5529822347005672810L;
        public UUID appUuid;
        
        public DropAppCheckpoint(final UUID appUuid) {
            this.appUuid = appUuid;
        }
        
        @Override
        public Boolean call() throws Exception {
            return StatusDataStore.getInstance().clearAppCheckpoint(this.appUuid);
        }
    }
}
