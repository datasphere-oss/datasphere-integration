package com.datasphere.event;

import org.apache.log4j.Logger;

import com.datalliance.event.ObjectMapperFactory;
import com.datalliance.event.SimpleEvent;
import com.datasphere.uuid.UUID;
import com.datasphere.waction.Waction;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventJson
{
    private static final long serialVersionUID = -8414735731457603959L;
    public static transient ObjectMapper jsonMapper;
    private static Logger logger;
    public UUID uuid;
    public String eventType;
    public String eventString;
    public String mapKey;
    
    public EventJson() {
        this.eventType = "SimpleEvent";
    }
    
    public EventJson(final SimpleEvent event) {
        this.eventType = "SimpleEvent";
        this.uuid = event.get_da_SimpleEvent_ID();
        this.eventString = event.toJSON();
        this.eventType = event.getClass().getCanonicalName();
    }
    
    public String getMapKey() {
        return this.mapKey;
    }
    
    public void setMapKey(final String mapKey) {
        this.mapKey = mapKey;
    }
    
    public String getEventIDStr() {
        return this.uuid.getUUIDString();
    }
    
    public void setEventIDStr(final String eventID) {
        this.uuid = new UUID(eventID);
    }
    
    public String getEventType() {
        return this.eventType;
    }
    
    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }
    
    public String getEventString() {
        return this.eventString;
    }
    
    public void setEventString(final String json) {
        this.eventString = json;
    }
    
    public Object buildEvent() {
        try {
            if (EventJson.logger.isTraceEnabled()) {
                EventJson.logger.trace((Object)("classname :" + this.eventType));
            }
            if (EventJson.logger.isTraceEnabled()) {
                EventJson.logger.trace((Object)("classname :" + ClassLoader.getSystemClassLoader().loadClass(this.eventType).getCanonicalName()));
            }
            if (EventJson.logger.isTraceEnabled()) {
                EventJson.logger.trace((Object)("json : " + this.eventString));
            }
            return EventJson.jsonMapper.readValue(this.eventString, (Class)ClassLoader.getSystemClassLoader().loadClass(this.eventType));
        }
        catch (Exception ex) {
            EventJson.logger.error((Object)("failed to build event object for type :" + this.eventType + "\n JSON = " + this.eventString), (Throwable)ex);
            return "<Undeserializable>";
        }
    }
    
    static {
        EventJson.jsonMapper = ObjectMapperFactory.newInstance();
        EventJson.logger = Logger.getLogger((Class)Waction.class);
    }
}
