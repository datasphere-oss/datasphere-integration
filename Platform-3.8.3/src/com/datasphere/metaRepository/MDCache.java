package com.datasphere.metaRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.event.ObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.map.listener.MapListener;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.HazelcastMultiMapListener;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.uuid.UUID;

public class MDCache implements MDCacheInterface, MembershipListener
{
    public static final int FIRST_VERSION = 1;
    private static MDCache INSTANCE;
    private static Logger logger;
    private boolean areMapsLoaded;
    private IMap<UUID, String> uuidToURL;
    private IMap<String, MetaInfo.MetaObject> urlToMetaObject;
    private MultiMap<Integer, String> eTypeToMetaObject;
    private IMap<UUID, MetaInfo.Server> servers;
    private IMap<UUID, MetaInfo.StatusInfo> status;
    private ITopic<MetaInfo.ShowStream> showStream;
    private MultiMap<UUID, UUID> deploymentInfo;
    private IMap<UUID, Position> positionInfo;
    private CacheStatistics statistics;
    
    private MDCache() {
        if (MDCache.INSTANCE != null) {
            throw new IllegalStateException("An instance of MDCache already exists");
        }
    }
    
    private MDCache(final boolean areMapsLoaded) {
        this.areMapsLoaded = areMapsLoaded;
        this.statistics = new CacheStatistics();
    }
    
    public static MDCache getInstance() {
        if (!MDCache.INSTANCE.areMapsLoaded) {
            MDCache.INSTANCE.initMaps();
        }
        return MDCache.INSTANCE;
    }
    
    public static MDCache getInstance(final String key, final String value) {
        if (!MDCache.INSTANCE.areMapsLoaded) {
            MDCache.INSTANCE.setInstanceProperties(key, value);
            MDCache.INSTANCE.initMaps();
        }
        return MDCache.INSTANCE;
    }
    
    public void setInstanceProperties(final String key, final String value) {
        System.setProperty(key, value);
    }
    
    private void initMaps() {
        if (!this.areMapsLoaded) {
            if (MDCache.logger.isDebugEnabled()) {
                MDCache.logger.debug("Loading IMaps first time from DB");
            }
            final HazelcastInstance hz = HazelcastSingleton.get();
            this.uuidToURL = hz.getMap("#uuidToUrl");
            this.urlToMetaObject = hz.getMap("#urlToMetaObject");
            this.eTypeToMetaObject = hz.getMultiMap("#eTypeToMetaObject");
            this.status = hz.getMap("#status");
            this.showStream = hz.getTopic("#showStream");
            this.deploymentInfo = hz.getMultiMap("deployedObjects");
            this.servers = hz.getMap("#servers");
            this.positionInfo = hz.getMap("#PositionInfo");
            this.areMapsLoaded = true;
            hz.getCluster().addMembershipListener((MembershipListener)this);
        }
    }
    
    public CacheStatistics getStatistics() {
        return this.statistics;
    }
    
    @Override
    public boolean put(final Object mObject) {
        final long start = System.currentTimeMillis();
        boolean success;
        if (mObject instanceof MetaInfo.MetaObject) {
            if (mObject instanceof MetaInfo.Server) {
                final MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)mObject;
                this.servers.put(metaObject.getUuid(), (MetaInfo.Server)mObject);
                this.eTypeToMetaObject.put(((MetaInfo.Server)mObject).type.ordinal(), ((MetaInfo.MetaObject)mObject).getUri().toUpperCase());
                success = this.servers.containsKey(metaObject.getUuid());
            }
            else {
                success = this.putMetaObjectInAllMaps((MetaInfo.MetaObject)mObject);
            }
        }
        else if (mObject instanceof MetaInfo.StatusInfo) {
            success = this.putStatusInfo((MetaInfo.StatusInfo)mObject);
        }
        else if (mObject instanceof MetaInfo.ShowStream) {
            this.showStream.publish((MetaInfo.ShowStream)mObject);
            success = true;
        }
        else if (mObject instanceof Pair) {
            final Pair<UUID, UUID> pair = (Pair<UUID, UUID>)mObject;
            success = this.deploymentInfo.put(pair.first, pair.second);
        }
        else if (mObject instanceof Map.Entry) {
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)mObject;
            if (entry.getKey() instanceof UUID && entry.getValue() instanceof Position) {
                final UUID key = (UUID)entry.getKey();
                final Position position = (Position)entry.getValue();
                this.positionInfo.put(key, position);
                success = (this.positionInfo.get(key) == position);
            }
            else {
                success = false;
            }
        }
        else {
            if (mObject == null) {
                throw new NullPointerException("Expects a non NULL Object to store in MetaData Cache!");
            }
            throw new UnsupportedOperationException("Unexpcted Object type : " + mObject.getClass() + ", will not store in MetaData Cache!");
        }
        final CacheStatistics statistics = this.statistics;
        ++statistics.cachePuts;
        final CacheStatistics statistics2 = this.statistics;
        statistics2.totalPutMillis += System.currentTimeMillis() - start;
        return success;
    }
    
    private boolean putMetaObjectInAllMaps(final MetaInfo.MetaObject mObject) {
        boolean success = true;
        final String uriInCaps = NamePolicy.makeKey(mObject.getUri());
        this.uuidToURL.put(mObject.getUuid(), uriInCaps);
        this.urlToMetaObject.put(uriInCaps, mObject);
        if (!mObject.getMetaInfoStatus().isDropped()) {
            this.eTypeToMetaObject.put(mObject.type.ordinal(), mObject.getUri().toUpperCase());
        }
        if (!this.uuidToURL.containsKey(mObject.uuid) || !this.urlToMetaObject.containsKey(mObject.getUri()) || !this.eTypeToMetaObject.containsKey(mObject.type.ordinal())) {
            success = false;
        }
        if (MDCache.logger.isDebugEnabled()) {
            MDCache.logger.debug((this.uuidToURL.entrySet() + "\n" + this.urlToMetaObject.entrySet()));
        }
        return success;
    }
    
    private boolean putStatusInfo(final MetaInfo.StatusInfo mObject) {
        if (MDCache.logger.isDebugEnabled()) {
            MDCache.logger.debug(("Putting status for " + mObject.type + " : " + mObject.OID));
        }
        boolean success = true;
        this.status.put(mObject.getOID(), mObject);
        if (!this.status.containsKey(mObject.getOID())) {
            success = false;
        }
        return success;
    }
    
    @Override
    public Object get(final EntityType eType, final UUID uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfGet get) {
        final long start = System.currentTimeMillis();
        Object result = null;
        switch (get) {
            case BY_NAME: {
                result = this.getByName(eType, namespace, name, version);
                break;
            }
            case BY_ENTITY_TYPE: {
                result = this.getByEntityType(eType);
                break;
            }
            case BY_NAMESPACE: {
                result = this.getByNamespace(namespace, null);
                break;
            }
            case BY_NAMESPACE_AND_ENTITY_TYPE: {
                result = this.getByNamespace(namespace, eType);
                break;
            }
            case BY_UUID: {
                result = this.getByUUID(uuid, name);
                break;
            }
        }
        if (result == null) {
            final CacheStatistics statistics = this.statistics;
            ++statistics.cacheMisses;
            final CacheStatistics statistics2 = this.statistics;
            statistics2.totalGetMillis += System.currentTimeMillis() - start;
        }
        else {
            final CacheStatistics statistics3 = this.statistics;
            ++statistics3.cacheHits;
            final CacheStatistics statistics4 = this.statistics;
            statistics4.totalGetMillis += System.currentTimeMillis() - start;
        }
        return result;
    }
    
    private MetaInfo.MetaObject getByName(final EntityType eType, final String namespace, final String name, final Integer version) {
        final String vUrl = MDConstants.makeURLWithVersion(eType, namespace, name, version);
        if (MDCache.logger.isDebugEnabled()) {
            MDCache.logger.debug(("Looking for: " + vUrl));
        }
        return (MetaInfo.MetaObject)this.urlToMetaObject.get(vUrl);
    }
    
    private Set<?> getByEntityType(final EntityType eType) {
        if (eType == EntityType.SERVER) {
            return new HashSet<Object>(this.servers.values());
        }
        final Collection<String> resultCollection = (Collection<String>)this.eTypeToMetaObject.get(eType.ordinal());
        if (resultCollection != null) {
            final Set<String> mSet = new HashSet<String>();
            mSet.addAll(resultCollection);
            return mSet;
        }
        if (MDCache.logger.isDebugEnabled()) {
            MDCache.logger.debug(("No Resultset for - " + eType));
        }
        return null;
    }
    
    private Set<MetaInfo.MetaObject> getByNamespace(final String namespace, final EntityType eType) {
        Set<MetaInfo.MetaObject> lObjects = null;
        if (eType != null) {
            final String searchTag = NamePolicy.makeKey(namespace + ":" + eType);
            if (MDCache.logger.isDebugEnabled()) {
                MDCache.logger.debug(("search Tag : " + searchTag));
            }
            for (final String tt : this.urlToMetaObject.keySet()) {
                if (tt.startsWith(searchTag)) {
                    if (lObjects == null) {
                        lObjects = new HashSet<MetaInfo.MetaObject>();
                    }
                    lObjects.add((MetaInfo.MetaObject)this.urlToMetaObject.get(tt));
                }
            }
        }
        else {
            final String searchTag = NamePolicy.makeKey(namespace + ":");
            if (MDCache.logger.isDebugEnabled()) {
                MDCache.logger.debug(("search Tag : " + searchTag));
            }
            for (final String tt : this.urlToMetaObject.keySet()) {
                if (tt.startsWith(searchTag)) {
                    if (lObjects == null) {
                        lObjects = new HashSet<MetaInfo.MetaObject>();
                    }
                    lObjects.add((MetaInfo.MetaObject)this.urlToMetaObject.get(tt));
                }
            }
        }
        return lObjects;
    }
    
    private Object getByUUID(final UUID uuid, final String status) {
        Object got = null;
        if (status != null && status.equalsIgnoreCase("status")) {
            got = this.status.get(uuid);
        }
        else if (status != null && status.equalsIgnoreCase("serversForDeployment")) {
            got = this.deploymentInfo.get(uuid);
        }
        else {
            final String vUrl = (String)this.uuidToURL.get(uuid);
            if (vUrl != null) {
                got = this.urlToMetaObject.get(vUrl);
            }
            if (got == null) {
                got = this.servers.get(uuid);
            }
        }
        return got;
    }
    
    @Override
    public Object remove(final EntityType eType, final Object uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfRemove remove) {
        final long start = System.currentTimeMillis();
        Object removed = null;
        switch (remove) {
            case BY_NAME: {
                removed = this.removeByName(eType, namespace, name, version);
                break;
            }
            case BY_UUID: {
                removed = this.removeByUUID(uuid, name);
                break;
            }
        }
        if (removed != null) {
            final CacheStatistics statistics = this.statistics;
            ++statistics.cacheRemovals;
        }
        final CacheStatistics statistics2 = this.statistics;
        statistics2.totalRemovalsMillis += System.currentTimeMillis() - start;
        return removed;
    }
    
    private Object removeByName(final EntityType eType, final String namespace, final String name, final Integer version) {
        final Object mObject = this.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
        if (mObject != null) {
            return this.removeMetaObjectFromAllMaps(mObject);
        }
        return mObject;
    }
    
    private Object removeByUUID(final Object objectID, final String name) {
        Object removed = null;
        if (objectID instanceof UUID) {
            Object mObject;
            if (name != null && name.equalsIgnoreCase("status")) {
                mObject = this.get(null, (UUID)objectID, null, name, null, MDConstants.typeOfGet.BY_UUID);
            }
            else {
                mObject = this.get(null, (UUID)objectID, null, null, null, MDConstants.typeOfGet.BY_UUID);
            }
            if (mObject != null) {
                removed = this.removeMetaObjectFromAllMaps(mObject);
            }
        }
        else if (objectID instanceof Pair) {
            final Pair<UUID, UUID> pair = (Pair<UUID, UUID>)objectID;
            final Object toBeRemoved = this.deploymentInfo.get(pair.first);
            if (toBeRemoved != null) {
                final Iterator iterator = ((Collection)toBeRemoved).iterator();
                if (iterator.hasNext()) {
                    removed = this.removeMetaObjectFromAllMaps(pair);
                    if (MDCache.logger.isDebugEnabled()) {
                        MDCache.logger.debug(("Removed : " + removed));
                        if ((boolean)removed) {
                            MDCache.logger.debug(("Removed Pair : " + pair));
                        }
                        else {
                            MDCache.logger.debug(("Could not remove Pair : " + pair));
                        }
                    }
                }
            }
        }
        else {
            MDCache.logger.error("Unknown type, can't remove based on Object ID.");
        }
        return removed;
    }
    
    private Object removeMetaObjectFromAllMaps(final Object object) {
        if (object instanceof MetaInfo.MetaObject) {
            final MetaInfo.MetaObject mObject = (MetaInfo.MetaObject)object;
            if (mObject instanceof MetaInfo.Server) {
                this.servers.remove(mObject.getUuid());
            }
            else {
                this.uuidToURL.remove(mObject.getUuid());
                this.urlToMetaObject.remove(mObject.getUri());
            }
            this.eTypeToMetaObject.remove(mObject.getType().ordinal(), mObject.getUri());
            return mObject;
        }
        if (object instanceof MetaInfo.StatusInfo) {
            final MetaInfo.StatusInfo statusInfo = (MetaInfo.StatusInfo)object;
            return this.status.remove(statusInfo.getOID());
        }
        if (object instanceof Pair) {
            final Pair<UUID, UUID> pair = (Pair<UUID, UUID>)object;
            return this.deploymentInfo.remove(pair.first, pair.second);
        }
        MDCache.logger.error(("Trying to remove object of unknown type: " + object.getClass()));
        return null;
    }
    
    public boolean update(final MetaInfo.MetaObject metaObject) {
        if (this.urlToMetaObject.containsKey(metaObject.getUri())) {
            this.urlToMetaObject.put(metaObject.getUri(), metaObject);
            if (metaObject.getMetaInfoStatus().isDropped()) {
                this.eTypeToMetaObject.remove(metaObject.getType().ordinal(), metaObject.getUri());
            }
            return true;
        }
        return false;
    }
    
    @Override
    public boolean contains(final MDConstants.typeOfRemove contains, final UUID uuid, final String url) {
        switch (contains) {
            case BY_UUID: {
                return this.uuidToURL.containsKey(uuid);
            }
            case BY_NAME: {
                return this.urlToMetaObject.containsKey(url);
            }
            default: {
                return false;
            }
        }
    }
    
    @Override
    public boolean clear() {
        boolean cleaned = true;
        this.uuidToURL.clear();
        this.urlToMetaObject.clear();
        this.eTypeToMetaObject.clear();
        if (!this.uuidToURL.isEmpty() || !this.urlToMetaObject.isEmpty() || this.eTypeToMetaObject.size() != 0) {
            cleaned = false;
        }
        return cleaned;
    }
    
    @Override
    public String registerListerForDeploymentInfo(final EntryListener<UUID, UUID> entryListener) {
        return this.deploymentInfo.addEntryListener((EntryListener)entryListener, true);
    }
    
    @Override
    public String registerListenerForShowStream(final MessageListener<MetaInfo.ShowStream> messageListener) {
        return this.showStream.addMessageListener((MessageListener)messageListener);
    }
    
    @Override
    public String registerListenerForStatusInfo(final HazelcastIMapListener mapListener) {
        return this.status.addEntryListener((MapListener)mapListener, true);
    }
    
    @Override
    public String registerListenerForMetaObject(final HazelcastIMapListener mapListener) {
        return this.urlToMetaObject.addEntryListener((MapListener)mapListener, true);
    }
    
    @Override
    public String registerListenerForServer(final HazelcastIMapListener mapListener) {
        return this.servers.addEntryListener((MapListener)mapListener, true);
    }
    
    @Override
    public String registerListenerForUUIDToUrlMap(final HazelcastIMapListener mapListener) {
        return this.uuidToURL.addEntryListener((MapListener)mapListener, true);
    }
    
    @Override
    public String registerListenerForEtypeToMetaObjectMap(final HazelcastMultiMapListener mapListener) {
        return this.eTypeToMetaObject.addEntryListener((EntryListener)mapListener, true);
    }
    
    @Override
    public void removeListerForDeploymentInfo(final String registrationId) {
        this.deploymentInfo.removeEntryListener(registrationId);
    }
    
    @Override
    public void removeListenerForShowStream(final String registrationid) {
        this.showStream.removeMessageListener(registrationid);
    }
    
    @Override
    public void removeListenerForStatusInfo(final String registrationId) {
        this.status.removeEntryListener(registrationId);
    }
    
    @Override
    public void removeListenerForMetaObject(final String registrationId) {
        this.urlToMetaObject.removeEntryListener(registrationId);
    }
    
    @Override
    public void removeListenerForServer(final String registrationId) {
        this.servers.removeEntryListener(registrationId);
    }
    
    public boolean shutDown() {
        MDCache.INSTANCE = null;
        return MDCache.INSTANCE == null;
    }
    
    public void reset() {
        MDCache.INSTANCE.areMapsLoaded = false;
        MDCache.INSTANCE.initMaps();
    }
    
    @Override
    public String registerCacheEntryListener(final EventListener eventListener, final MDConstants.mdcListener listener) {
        switch (listener) {
            case deploymentInfo: {
                return this.deploymentInfo.addEntryListener((EntryListener)eventListener, true);
            }
            case showStream: {
                return this.showStream.addMessageListener((MessageListener)eventListener);
            }
            case status: {
                return this.status.addEntryListener((EntryListener)eventListener, true);
            }
            case urlToMeta: {
                return this.urlToMetaObject.addEntryListener((EntryListener)eventListener, true);
            }
            case server: {
                return this.servers.addEntryListener((EntryListener)eventListener, true);
            }
            default: {
                return null;
            }
        }
    }
    
    public String exportMetadataAsJson() {
        try {
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            if (MDCache.logger.isInfoEnabled()) {
                MDCache.logger.info(("urlToMetaObject.size : " + this.urlToMetaObject.size()));
            }
            final String json = this.getjsonfrommetaobject();
            MDCache.logger.warn(("Number of Meta objects exported as JSON: " + this.urlToMetaObject.size() + ", and json size:" + json.length()));
            if (MDCache.logger.isInfoEnabled()) {
                MDCache.logger.info(("json doc size : " + json.length()));
            }
            return json;
        }
        catch (JSONException | IOException ex2) {
            MDCache.logger.error(ex2);
            return null;
        }
    }
    
    private String getjsonfrommetaobject() throws JsonProcessingException, JSONException {
        final Map<String, JSONObject> map = new Hashtable<String, JSONObject>();
        String json = "";
        final Set<String> keys = (Set<String>)this.urlToMetaObject.keySet();
        if (keys == null || keys.size() == 0) {
            MDCache.logger.warn("no meta objects exists");
            return json;
        }
        final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
        jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        final List<String> ignoredList = Arrays.asList("GLOBAL:TYPE:MONITORBATCHEVENT", "GLOBAL:SOURCE:", "GLOBAL:STREAM:", "GLOBAL:CQ:", "GLOBAL:APPLICATION:", "GLOBAL:FLOW:");
        for (final String key : keys) {
            boolean skipKey = false;
            for (final String anIgnoredList : ignoredList) {
                if (key.startsWith(anIgnoredList)) {
                    skipKey = true;
                    break;
                }
            }
            if (skipKey) {
                continue;
            }
            json = json + "\"" + key + "\" : ";
            if (key.contains(":" + EntityType.QUERY.name().toUpperCase() + ":")) {
                json += ((MetaInfo.MetaObject)this.urlToMetaObject.get(key)).JSONify();
            }
            else if (key.contains(":" + EntityType.TYPE.name().toUpperCase() + ":")) {
                final String result = ((MetaInfo.MetaObject)this.urlToMetaObject.get(key)).JSONifyString();
                json += result;
            }
            else {
                json += jsonMapper.writeValueAsString(this.urlToMetaObject.get(key));
            }
            json += ",";
        }
        json = json.substring(0, json.length() - 1);
        return "{\n" + json + "\n}";
    }
    
    public void importMetadataFromJson(final String json, final Boolean replace) {
        try {
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            final JsonNode listNode = jsonMapper.readTree(json);
            if (!listNode.isContainerNode()) {
                MDCache.logger.warn("JSON MetaData is not in list format");
                return;
            }
            final Set<MetaInfo.MetaObject> moSet = new HashSet<MetaInfo.MetaObject>();
            final Iterator<JsonNode> it = (Iterator<JsonNode>)listNode.elements();
            while (it.hasNext()) {
                try {
                    final JsonNode moNode = it.next();
                    final String className = moNode.get("metaObjectClass").asText();
                    final String moNodeText = moNode.toString();
                    final Class<?> moClass = Class.forName(className, false, ClassLoader.getSystemClassLoader());
                    final Object ob = jsonMapper.readValue(moNodeText, (Class)moClass);
                    final MetaInfo.MetaObject mo = (MetaInfo.MetaObject)ob;
                    moSet.add(mo);
                }
                catch (ClassNotFoundException e) {
                    MDCache.logger.error(e, (Throwable)e);
                }
                catch (Exception e2) {
                    MDCache.logger.error(e2.getMessage());
                }
            }
            if (replace) {
                this.urlToMetaObject.clear();
            }
            MDCache.logger.warn(("----- MDC READY TO IMPORT OBJECTS: #" + moSet.size()));
            final EntityType[] array;
            final EntityType[] orderedTypes = array = new EntityType[] { EntityType.USER, EntityType.ROLE, EntityType.TYPE, EntityType.STREAM, EntityType.CQ, EntityType.TYPE, EntityType.SOURCE, EntityType.TARGET, EntityType.WI, EntityType.PROPERTYSET, EntityType.HDSTORE, EntityType.CACHE, EntityType.SERVER, EntityType.INITIALIZER, EntityType.DG, EntityType.VISUALIZATION, EntityType.WINDOW, EntityType.STREAM, EntityType.APPLICATION, EntityType.NAMESPACE };
            for (final EntityType entityType : array) {
                final Set<MetaInfo.MetaObject> typedMos = new HashSet<MetaInfo.MetaObject>();
                for (final MetaInfo.MetaObject mo2 : moSet) {
                    if (mo2.type == entityType) {
                        typedMos.add(mo2);
                    }
                }
                for (final MetaInfo.MetaObject mo2 : typedMos) {
                    MDCache.logger.warn(("----- MDC IMPORT type=" + entityType + " mo=" + mo2));
                    this.urlToMetaObject.put(mo2.uri, mo2);
                }
                moSet.removeAll(typedMos);
            }
            for (final MetaInfo.MetaObject mo3 : moSet) {
                MDCache.logger.warn(("----- MDC IMPORT THE REST mo=" + mo3));
                this.urlToMetaObject.put(mo3.uri, mo3);
            }
        }
        catch (IOException e3) {
            MDCache.logger.error(e3, (Throwable)e3);
        }
    }
    
    public IMap<UUID, String> getUuidToURL() {
        return this.uuidToURL;
    }
    
    public IMap<String, MetaInfo.MetaObject> getUrlToMetaObject() {
        return this.urlToMetaObject;
    }
    
    public MultiMap<Integer, String> geteTypeToMetaObject() {
        return this.eTypeToMetaObject;
    }
    
    public IMap<UUID, MetaInfo.StatusInfo> getStatus() {
        return this.status;
    }
    
    public ITopic<MetaInfo.ShowStream> getShowStream() {
        return this.showStream;
    }
    
    public MultiMap<UUID, UUID> getDeploymentInfo() {
        return this.deploymentInfo;
    }
    
    public IMap<UUID, Position> getPositionInfo() {
        return this.positionInfo;
    }
    
    public void memberAdded(final MembershipEvent event) {
    }
    
    public void memberAttributeChanged(final MemberAttributeEvent event) {
    }
    
    public void memberRemoved(final MembershipEvent event) {
        final UUID leftUUID = new UUID(event.getMember().getUuid());
        final List<Pair<UUID, UUID>> toRemove = new ArrayList<Pair<UUID, UUID>>();
        for (final Map.Entry<UUID, UUID> entry : this.deploymentInfo.entrySet()) {
            if (leftUUID.equals(entry.getValue())) {
                toRemove.add(Pair.make(entry.getKey(), entry.getValue()));
            }
        }
        for (final Pair<UUID, UUID> pair : toRemove) {
            this.deploymentInfo.remove(pair.first, pair.second);
        }
    }
    
    static {
        MDCache.INSTANCE = new MDCache(false);
        MDCache.logger = Logger.getLogger((Class)MDCache.class);
    }
}
