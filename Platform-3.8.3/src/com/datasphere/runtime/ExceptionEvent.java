package com.datasphere.runtime;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.runtime.components.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.appmanager.*;
import com.datasphere.event.*;
import com.datasphere.exceptionhandling.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class ExceptionEvent extends SimpleEvent
{
    private static final long serialVersionUID = 4645917522060103160L;
    private static Logger logger;
    public ExceptionType type;
    public ActionType action;
    public UUID appid;
    public EntityType entityType;
    public String entityName;
    public UUID entityId;
    public AppManager.ComponentStatus componentStatus;
    public String className;
    public String message;
    public Long epochNumber;
    public String relatedActivity;
    public String[] relatedObjects;
    
    public ExceptionEvent() {
        this.componentStatus = AppManager.ComponentStatus.UNKNOWN;
        this.epochNumber = new Long(-1L);
    }
    
    public ExceptionEvent(final UUID appid, final ExceptionType exceptionType, final ActionType action) {
        this.componentStatus = AppManager.ComponentStatus.UNKNOWN;
        this.epochNumber = new Long(-1L);
        this.appid = appid;
        this.type = exceptionType;
        this.action = action;
    }
    
    public ExceptionType getType() {
        return this.type;
    }
    
    public void setType(final ExceptionType exceptionType) {
        this.type = exceptionType;
    }
    
    public ActionType getAction() {
        return this.action;
    }
    
    public void setAction(final ActionType action) {
        this.action = action;
    }
    
    public Long getEpochNumber() {
        return this.epochNumber;
    }
    
    public void setEpochNumber(final Long epochNumber) {
        this.epochNumber = epochNumber;
    }
    
    public UUID getAppid() {
        return this.appid;
    }
    
    public void setAppid(final UUID appid) {
        this.appid = appid;
    }
    
    public EntityType getEntityType() {
        return this.entityType;
    }
    
    public void setEntityType(final EntityType entityType) {
        this.entityType = entityType;
    }
    
    public AppManager.ComponentStatus getComponentStatus() {
        return this.componentStatus;
    }
    
    public void setComponentStatus(final AppManager.ComponentStatus componentStatus) {
        this.componentStatus = componentStatus;
    }
    
    public void setClassName(final String className) {
        this.className = className;
    }
    
    public void setMessage(final String message) {
        this.message = message;
    }
    
    public String toString() {
        final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
        jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        final StringBuilder json = new StringBuilder();
        try {
            json.append("\"componentName\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.entityName));
            json.append(" , ");
            json.append("\"componentType\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.entityType));
            json.append(" , ");
            json.append("\"exception\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.className));
            json.append(" , ");
            json.append("\"message\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.message));
            json.append(" , ");
            json.append("\"relatedEvents\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.relatedObjects));
            json.append(" , ");
            json.append("\"action\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.action));
            json.append(" , ");
            json.append("\"exceptionType\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.type));
            json.append(" , ");
            json.append("\"epochNumber\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.epochNumber));
        }
        catch (JsonProcessingException e) {
            ExceptionEvent.logger.error((Object)("Failed to jsonify relatedObjects in exceptionEvent with exception " + e.getMessage()));
        }
        return "ExceptionEvent : {\n" + json.toString() + "\n}";
    }
    
    public String userString() {
        final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
        jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        final StringBuilder json = new StringBuilder();
        try {
            json.append("\"componentName\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.entityName));
            json.append(" , ");
            json.append("\"componentType\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.entityType));
            json.append(" , ");
            json.append("\"exception\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.className));
            json.append(" , ");
            json.append("\"message\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.message));
            json.append(" , ");
            json.append("\"relatedEvents\" : ");
            json.append(jsonMapper.writeValueAsString((Object)this.relatedObjects));
        }
        catch (JsonProcessingException e) {
            ExceptionEvent.logger.error((Object)("Failed to jsonify relatedObjects in exceptionEvent with exception " + e.getMessage()));
        }
        return "{\n" + json.toString() + "\n}";
    }
    
    public String parseRelatedObjects() {
        final StringBuilder builder = new StringBuilder();
        for (final String s : this.relatedObjects) {
            builder.append(s);
        }
        final String relatedObjectsAsString = builder.toString();
        String removeParenthesis = new String(relatedObjectsAsString);
        removeParenthesis = removeParenthesis.substring(removeParenthesis.indexOf("(") + 1);
        removeParenthesis = removeParenthesis.substring(0, removeParenthesis.indexOf(")"));
        String removeBrackets = new String(removeParenthesis);
        removeBrackets = removeBrackets.substring(removeBrackets.indexOf("[") + 1);
        removeBrackets = removeBrackets.substring(0, removeBrackets.indexOf("] ["));
        return removeBrackets;
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeObjectOrNull(output, (Object)this.type, (Class)ExceptionType.class);
        kryo.writeObjectOrNull(output, (Object)this.action, (Class)ActionType.class);
        kryo.writeObjectOrNull(output, (Object)this.appid, (Class)UUID.class);
        kryo.writeObjectOrNull(output, (Object)this.entityType, (Class)EntityType.class);
        kryo.writeObjectOrNull(output, (Object)this.entityId, (Class)UUID.class);
        kryo.writeObjectOrNull(output, (Object)this.componentStatus, (Class)AppManager.ComponentStatus.class);
        output.writeString(this.className);
        output.writeString(this.message);
        output.writeString(this.entityName);
        kryo.writeObjectOrNull(output, (Object)this.epochNumber, (Class)Long.class);
        output.writeString(this.relatedActivity);
        output.writeBoolean(this.relatedObjects != null);
        if (this.relatedObjects != null) {
            output.writeInt(this.relatedObjects.length);
            for (final String o : this.relatedObjects) {
                output.writeString(o);
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.type = (ExceptionType)kryo.readObjectOrNull(input, (Class)ExceptionType.class);
        this.action = (ActionType)kryo.readObjectOrNull(input, (Class)ActionType.class);
        this.appid = (UUID)kryo.readObjectOrNull(input, (Class)UUID.class);
        this.entityType = (EntityType)kryo.readObjectOrNull(input, (Class)EntityType.class);
        this.entityId = (UUID)kryo.readObjectOrNull(input, (Class)UUID.class);
        this.componentStatus = (AppManager.ComponentStatus)kryo.readObjectOrNull(input, (Class)AppManager.ComponentStatus.class);
        this.className = input.readString();
        this.message = input.readString();
        this.entityName = input.readString();
        this.epochNumber = (Long)kryo.readObjectOrNull(input, (Class)Long.class);
        this.relatedActivity = input.readString();
        final boolean hasRelatedObjects = input.readBoolean();
        if (hasRelatedObjects) {
            final int numRelatedObjects = input.readInt();
            this.relatedObjects = new String[numRelatedObjects];
            for (int i = 0; i < numRelatedObjects; ++i) {
                this.relatedObjects[i] = input.readString();
            }
        }
    }
    
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    public void setRelatedObjects(final Object[] relatedObjects) {
        if (relatedObjects == null) {
            return;
        }
        this.relatedObjects = new String[relatedObjects.length];
        for (int i = 0; i < relatedObjects.length; ++i) {
            this.relatedObjects[i] = relatedObjects[i].toString();
        }
    }
    
    static {
        ExceptionEvent.logger = Logger.getLogger((Class)ExceptionEvent.class);
    }
}
