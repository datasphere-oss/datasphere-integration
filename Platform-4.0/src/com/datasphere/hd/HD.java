package com.datasphere.hd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.persistence.queries.FetchGroup;
import org.eclipse.persistence.queries.FetchGroupTracker;
import org.eclipse.persistence.sessions.Session;
import org.joda.time.DateTime;

import com.datasphere.anno.SpecialEventAttribute;
import com.datasphere.event.Event;
import com.datasphere.event.EventJson;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.event.SimpleEvent;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.RecordKey;
import com.datasphere.uuid.UUID;

import flexjson.JSON;

public class HD implements Serializable, FetchGroupTracker
{
    private static final long serialVersionUID = 383497383255254943L;
    private static Logger logger;
    private static transient ObjectMapper mapper;
    public static final int STATUS_DEFAULT = 0;
    public static final int STATUS_COMMITTED = 2;
    public static final int STATUS_PERSISTED = 4;
    public String _id;
    public String id;
    public String internalHDStoreName;
    public UUID uuid;
    public long hdTs;
    public Object key;
    public HDKey mapKey;
    public HDContext context;
    private List<SimpleEvent> eventsNotAccessedDirectly;
    @JSON(include = false)
    @JsonIgnore
    private List<EventJson> jsonEventListNotAccessedDirectly;
    public int hdStatus;
    @SpecialEventAttribute
    private Position position;
    transient List<HDListener> hdListeners;
    private boolean notificationFlag;
    transient FetchGroup fetchGroup;
    transient Session session;
    
    public HD() {
        this.eventsNotAccessedDirectly = null;
        this.jsonEventListNotAccessedDirectly = new ArrayList<EventJson>();
        this.hdStatus = 0;
        this.hdListeners = new ArrayList<HDListener>();
        this.notificationFlag = false;
        this.fetchGroup = null;
        this.context = new HDContext();
    }
    
    public HD(final Object key) {
        this();
        this.init(key);
    }
    
    public HD(final UUID uuid, final Object key, final long ts, final List<SimpleEvent> events, final int status, final HDContext ctx) {
        this.eventsNotAccessedDirectly = null;
        this.jsonEventListNotAccessedDirectly = new ArrayList<EventJson>();
        this.hdStatus = 0;
        this.hdListeners = new ArrayList<HDListener>();
        this.notificationFlag = false;
        this.fetchGroup = null;
        this.setHDTs(ts);
        this.setUuid(uuid);
        this.id = this.getUUIDString();
        this.setKey(key);
        this.setMapKey(new HDKey(uuid, key));
        this.setHDStatus(status);
        if (events != null) {
            this.setEvents(events);
        }
        if (ctx != null) {
            this.setContext(ctx);
        }
    }
    
    public void init(final Object key) {
        this.setHDTs(System.currentTimeMillis());
        this.setUuid(new UUID(this.hdTs));
        this.id = this.getUUIDString();
        if (key instanceof RecordKey && ((RecordKey)key).isEmpty()) {
            this.setKey(this.id);
        }
        else {
            this.setKey(key);
        }
        this.setMapKey(new HDKey(this.uuid, key));
        this.setEvents(new ArrayList<SimpleEvent>());
    }
    
    public String getId() {
        return this.getUUIDString();
    }
    
    public void setId(final String id) {
        this.id = id;
        this.uuid = new UUID(id);
    }
    
    public String getInternalHDStoreName() {
        return this.internalHDStoreName;
    }
    
    public void setInternalHDStoreName(final String storeName) {
        this.internalHDStoreName = storeName;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public byte[] getEventsString() {
        if (this.eventsNotAccessedDirectly == null || this.eventsNotAccessedDirectly.size() < 1) {
            return "{\"list\" :[] }".getBytes();
        }
        final int ne = this.eventsNotAccessedDirectly.size();
        if (HD.logger.isTraceEnabled()) {
            HD.logger.trace((Object)("No of events in list to create json: " + ne));
        }
        String json = new String();
        final HDEventsClassList wecl = new HDEventsClassList();
        try {
            for (int ik = 0; ik < ne; ++ik) {
                final SimpleEvent se = this.eventsNotAccessedDirectly.get(ik);
                final String eventClassName = se.getClass().getCanonicalName();
                final String eventJson = se.toJSON();
                final HDEventsClass wec = new HDEventsClass(eventClassName, eventJson);
                if (HD.logger.isTraceEnabled()) {
                    HD.logger.trace((Object)("Event of type : " + eventClassName + ", is written as json : " + eventJson));
                }
                wecl.add(wec);
            }
            json = HD.mapper.writeValueAsString((Object)wecl);
        }
        catch (Exception e) {
            HD.logger.error((Object)("error writing events as json" + e));
        }
        if (HD.logger.isTraceEnabled()) {
            HD.logger.trace((Object)("events as json string : " + json));
        }
        return json.getBytes();
    }
    
    public void setEventsString(final byte[] bytes) {
        this.setEvents(new ArrayList<SimpleEvent>());
        final String jsonStr = new String(bytes);
        try {
            final JsonNode rootNode = (JsonNode)HD.mapper.readValue(jsonStr, (Class)JsonNode.class);
            if (rootNode == null) {
                HD.logger.error((Object)"failed to build events list from json.");
                return;
            }
            final JsonNode listNode = rootNode.get("list");
            if (listNode.isArray()) {
                final Iterator<JsonNode> nodes = (Iterator<JsonNode>)listNode.elements();
                while (nodes.hasNext()) {
                    final JsonNode node = nodes.next();
                    final String eventClassName = node.get("eventClassName").asText();
                    final String eventNodeStr = node.get("eventJson").asText();
                    final Class<?> seClazz = ClassLoader.getSystemClassLoader().loadClass(eventClassName);
                    Object seObj = seClazz.newInstance();
                    seObj = seClazz.cast(seObj);
                    final SimpleEvent se = (SimpleEvent)((SimpleEvent)seObj).fromJSON(eventNodeStr);
                    this.eventsNotAccessedDirectly.add(se);
                }
            }
        }
        catch (Exception e) {
            HD.logger.error((Object)("error building events list : " + e));
        }
        if (HD.logger.isTraceEnabled()) {
            HD.logger.trace((Object)("no of events in events list built are : " + this.eventsNotAccessedDirectly.size()));
        }
    }
    
    @JSON(include = false)
    @JsonIgnore
    public String getContextString() {
        final String contextString = (null == this.context || null == this.context.toString()) ? "empty context" : this.context.toString();
        return contextString;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public String getKeyString() {
        final String keyString = (this.key == null || null == this.key.toString()) ? "Object key is NULL" : this.key.toString();
        return keyString;
    }
    
    public void setKeyString(final String key) {
        this.key = key;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public String getMapKeyString() {
        String mapKeyString = null;
        if (this.mapKey != null) {
            mapKeyString = this.mapKey.getHDKeyStr();
        }
        return mapKeyString;
    }
    
    public void setMapKeyString(final String mapKeyString) {
        (this.mapKey = new HDKey()).setHDKey(mapKeyString);
    }
    
    public UUID getUuid() {
        return this.uuid;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public String getUUIDString() {
        return this.uuid.getUUIDString();
    }
    
    public void setUUIDString(final String uuid) {
        this.uuid = new UUID(uuid);
    }
    
    public void setUuid(final UUID uuid) {
        this.uuid = uuid;
    }
    
    public long getHDTs() {
        return this.hdTs;
    }
    
    public void setHDTs(final long hdTs) {
        this.hdTs = hdTs;
    }
    
    public Object getKey() {
        return this.key;
    }
    
    public void setKey(final Object key) {
        this.key = key;
        this.setMapKey(new HDKey(this.uuid, key));
    }
    
    public HDKey getMapKey() {
        return this.mapKey;
    }
    
    public void setMapKey(final HDKey mapKey) {
        this.mapKey = mapKey;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public List<EventJson> getJsonEvents() {
        if (this.eventsNotAccessedDirectly == null || this.eventsNotAccessedDirectly.size() == 0 || this.mapKey == null) {
            return null;
        }
        this.jsonEventListNotAccessedDirectly = new ArrayList<EventJson>();
        for (final SimpleEvent event : this.eventsNotAccessedDirectly) {
            final EventJson ej = new EventJson(event);
            ej.setMapKey(this.mapKey.getHDKeyStr());
            this.jsonEventListNotAccessedDirectly.add(ej);
        }
        if (HD.logger.isTraceEnabled()) {
            HD.logger.trace((Object)("no of jsonEvents in hd=" + this.mapKey.getHDKeyStr() + ", are=" + this.jsonEventListNotAccessedDirectly.size()));
        }
        return this.jsonEventListNotAccessedDirectly;
    }
    
    public void setJsonEvents(final List<EventJson> jsonEvents) {
        if (jsonEvents == null || jsonEvents.size() == 0) {
            return;
        }
        this.jsonEventListNotAccessedDirectly = jsonEvents;
        this.eventsNotAccessedDirectly = new ArrayList<SimpleEvent>();
        for (final EventJson je : jsonEvents) {
            final SimpleEvent se = (SimpleEvent)je.buildEvent();
            this.eventsNotAccessedDirectly.add(se);
        }
    }
    
    public List<SimpleEvent> getEvents() {
        if ((this.eventsNotAccessedDirectly == null || this.eventsNotAccessedDirectly.size() == 0) && this.jsonEventListNotAccessedDirectly != null && this.jsonEventListNotAccessedDirectly.size() > 0) {
            this.setJsonEvents(this.jsonEventListNotAccessedDirectly);
        }
        return this.eventsNotAccessedDirectly;
    }
    
    public void setEvents(final List<SimpleEvent> events) {
        this.notifyHDAccessed();
        this.eventsNotAccessedDirectly = events;
    }
    
    public void addEvent(final SimpleEvent event) {
        this.eventsNotAccessedDirectly.add(event);
    }
    
    public List<Event> getEvents(final int start, final int stop) {
        final List<Event> eventList = new ArrayList<Event>();
        for (int i = start; i <= stop; ++i) {
            eventList.add((Event)this.eventsNotAccessedDirectly.get(i));
        }
        return eventList;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public Map<String, Object> getContext() {
        return this.context;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public Object getContextField(final String name) {
        return this.context.get(name);
    }
    
    public void setContext(final Object obj) {
        if (obj instanceof Map) {
            this.setContext((Map<String, Object>)obj);
        }
    }
    
    public void setContext(final Map<String, Object> context) {
        if (context != null) {
            this.context.putAll(context);
        }
    }
    
    public void setContext(final HDContext context) {
        this.context = context;
    }
    
    public int numberOfEvents() {
        if (this.eventsNotAccessedDirectly == null) {
            return 0;
        }
        return this.eventsNotAccessedDirectly.size();
    }
    
    public Object fromJSON(final String json) {
        try {
            return HD.mapper.readValue(json, (Class)this.getClass());
        }
        catch (Exception ex) {
            return "<Undeserializable>";
        }
    }
    
    public String toJSON() {
        try {
            return HD.mapper.writeValueAsString((Object)this);
        }
        catch (Exception ex) {
            return "<Undeserializable>";
        }
    }
    
    public JsonNode toJSONNode() {
        try {
            return (JsonNode)HD.mapper.convertValue((Object)this, (Class)JsonNode.class);
        }
        catch (Exception ex) {
            return null;
        }
    }
    
    @Override
    public String toString() {
        return " uuid = " + this.uuid.toString() + ", key =" + this.key + ", timestamp = " + this.hdTs + ", _id = " + this._id + ", context = " + this.getContextString() + ", position = " + this.position;
    }
    
    public void designateCommitted() {
        this.hdStatus |= 0x2;
        this.notifyHDAccessed();
    }
    
    public void designatePersisted() {
        this.hdStatus |= 0x4;
        this.notifyHDAccessed();
    }
    
    @JSON(include = false)
    @JsonIgnore
    public boolean isPersisted() {
        return (this.hdStatus & 0x4) == 0x4;
    }
    
    public int getHDStatus() {
        return this.hdStatus;
    }
    
    public void setHDStatus(final int hdStatus) {
        this.hdStatus = hdStatus;
    }
    
    public void addHDListener(final HDListener listener) {
        this.hdListeners.add(listener);
    }
    
    protected Object getWrappedValue(final boolean val) {
        return new Boolean(val);
    }
    
    protected Object getWrappedValue(final char val) {
        return new Character(val);
    }
    
    protected Object getWrappedValue(final short val) {
        return new Short(val);
    }
    
    protected Object getWrappedValue(final int val) {
        return new Integer(val);
    }
    
    protected Object getWrappedValue(final long val) {
        return new Long(val);
    }
    
    protected Object getWrappedValue(final float val) {
        return new Float(val);
    }
    
    protected Object getWrappedValue(final double val) {
        return new Double(val);
    }
    
    protected Object getWrappedValue(final Object val) {
        return val;
    }
    
    protected String getWrappedValue(final String val) {
        if (val == null) {
            return null;
        }
        return new String(val);
    }
    
    protected Object getWrappedValue(final DateTime val) {
        return val;
    }
    
    public synchronized void notifyHDAccessed() {
        if (this.notificationFlag) {
            return;
        }
        this.notificationFlag = true;
        for (final HDListener listener : this.hdListeners) {
            listener.hdAccessed(this);
        }
        this.notificationFlag = false;
    }
    
    public Position getPosition() {
        return this.position;
    }
    
    public void setPosition(final Position position) {
        this.position = position;
    }
    
    public FetchGroup _persistence_getFetchGroup() {
        return this.fetchGroup;
    }
    
    public void _persistence_setFetchGroup(final FetchGroup group) {
        this.fetchGroup = group;
    }
    
    public boolean _persistence_isAttributeFetched(final String attribute) {
        boolean fetched = false;
        if (this.fetchGroup != null) {
            fetched = this.fetchGroup.containsAttribute(attribute);
        }
        return fetched;
    }
    
    public void _persistence_resetFetchGroup() {
    }
    
    public boolean _persistence_shouldRefreshFetchGroup() {
        return false;
    }
    
    public void _persistence_setShouldRefreshFetchGroup(final boolean shouldRefreshFetchGroup) {
    }
    
    public Session _persistence_getSession() {
        return this.session;
    }
    
    public void _persistence_setSession(final Session session) {
        this.session = session;
    }
    
    static {
        HD.logger = Logger.getLogger((Class)HD.class);
        HD.mapper = ObjectMapperFactory.newInstance();
    }
    
    class HDEventsClass
    {
        String eventClassName;
        String eventJson;
        
        HDEventsClass(final String eventClassName, final String eventJson) {
            this.eventClassName = eventClassName;
            this.eventJson = eventJson;
        }
    }
    
    class HDEventsClassList
    {
        public List<HDEventsClass> list;
        
        HDEventsClassList() {
            this.list = new ArrayList<HDEventsClass>();
        }
        
        public void add(final HDEventsClass wec) {
            this.list.add(wec);
        }
    }
}
