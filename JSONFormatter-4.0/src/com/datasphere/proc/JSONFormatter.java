package com.datasphere.proc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.classloading.ParserLoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.event.Event;
import com.datasphere.intf.Parser;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.containers.HDEvent;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.FieldModifier;
import com.datasphere.source.lib.utils.JSONObjectMember;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.JsonNodeEvent;
import com.fasterxml.jackson.databind.JsonNode;

@PropertyTemplate(name = "JSONFormatter", type = AdapterType.formatter, properties = { @PropertyTemplateProperty(name = "jsonobjectdelimiter", type = String.class, required = false, defaultValue = "\n"), @PropertyTemplateProperty(name = "jsonMemberDelimiter", type = String.class, required = false, defaultValue = "\n"), @PropertyTemplateProperty(name = "EventsAsArrayOfJsonObjects", type = Boolean.class, required = false, defaultValue = "true"), @PropertyTemplateProperty(name = "charset", type = String.class, required = false, defaultValue = ""), @PropertyTemplateProperty(name = "members", type = String.class, required = false, defaultValue = "") })
public class JSONFormatter extends BaseFormatter
{
    private List<JSONObjectMember.Member> memberList;
    private String jsonMemberDelimiter;
    private String jsonObjectDelimiter;
    private String eventSeparator;
    private boolean formatEventsAsArrayOfJsonObjects;
    private Logger logger;
    private volatile boolean firstCall;
    private String dataexchangeMetadata;
    
    public JSONFormatter(final Map<String, Object> formatterProperties, final Field[] fields) throws AdapterException {
        super((Map)formatterProperties, fields);
        this.eventSeparator = "";
        this.formatEventsAsArrayOfJsonObjects = true;
        this.logger = Logger.getLogger((Class)JSONFormatter.class);
        this.firstCall = true;
        this.dataexchangeMetadata = "";
        final String memberList = (String)formatterProperties.get("members");
        this.jsonMemberDelimiter = (String)formatterProperties.get("jsonMemberDelimiter");
        this.formatEventsAsArrayOfJsonObjects = (boolean)formatterProperties.get("EventsAsArrayOfJsonObjects");
        if (this.formatEventsAsArrayOfJsonObjects) {
            this.eventSeparator = ",";
        }
        this.jsonObjectDelimiter = (String)formatterProperties.get("jsonobjectdelimiter");
        this.fieldModifiers = FieldModifier.getFieldModifiers((Map)formatterProperties, fields, "json");
        final JSONObjectMember member = new JSONObjectMember(memberList, fields, this.fieldModifiers);
        this.memberList = (List<JSONObjectMember.Member>)member.getMemberList();
        formatterProperties.put(Property.CHARSET, this.charset);
        super.validHeaderAndFooter = true;
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("JSONFormatter is initialized with following properties\nMembers - [" + memberList + "]\nCharset - [" + this.charset + "]\nJSONMemberDelimiter - [" + this.jsonMemberDelimiter + "]JSONObjectDelimiter - [" + this.jsonObjectDelimiter + "]EventsAsArrayOfJsonObjects - [" + this.formatEventsAsArrayOfJsonObjects + "]"));
        }
    }
    
    public byte[] format(Object event) throws Exception {
        if (this.exactlyOnceProcessing) {
            final HDEvent containerEvent = (HDEvent)event;
            event = containerEvent.data;
            String position = "null";
            if (containerEvent.position != null) {
                position = new String(Base64.encodeBase64(KryoSingleton.writeImmutableStemma(containerEvent.position)));
                position = "\"" + position + "\"";
            }
            final StringBuilder dataexchangeMetadataBuilder = new StringBuilder();
            dataexchangeMetadataBuilder.append(",").append(this.jsonMemberDelimiter).append("  ").append("\"__dataexchangemetadata\"").append(":").append(" {").append(this.jsonMemberDelimiter).append("  ").append("\"position\"").append(":").append(position).append(this.jsonMemberDelimiter).append(" }");
            this.dataexchangeMetadata = dataexchangeMetadataBuilder.toString();
        }
        final StringBuilder stringBuilder = new StringBuilder();
        if (this.firstCall) {
            stringBuilder.append(" {").append(this.jsonMemberDelimiter);
            this.firstCall = false;
        }
        else {
            stringBuilder.append(this.eventSeparator).append(this.jsonObjectDelimiter).append(" {").append(this.jsonMemberDelimiter);
        }
        for (int i = 0; i < this.memberList.size(); ++i) {
            final JSONObjectMember.Member member = this.memberList.get(i);
            if (i < this.memberList.size() - 1) {
                stringBuilder.append("  ").append(member.getName()).append(member.processValue(event)).append(",").append(this.jsonMemberDelimiter);
            }
            else {
                stringBuilder.append("  ").append(member.getName()).append(member.processValue(event));
            }
        }
        stringBuilder.append(this.dataexchangeMetadata);
        stringBuilder.append(this.jsonMemberDelimiter);
        stringBuilder.append(" }");
        return stringBuilder.toString().getBytes(this.charset);
    }
    
    public byte[] addHeader() throws Exception {
        this.firstCall = true;
        if (this.formatEventsAsArrayOfJsonObjects) {
            return ("[" + this.jsonObjectDelimiter).getBytes(this.charset);
        }
        return "".getBytes(this.charset);
    }
    
    public byte[] addFooter() throws Exception {
        if (this.formatEventsAsArrayOfJsonObjects) {
            return (this.jsonObjectDelimiter + "]").getBytes(this.charset);
        }
        return "".getBytes(this.charset);
    }
    
    public Position convertBytesToPosition(final byte[] bytes, final UUID targetUUID) throws Exception {
        final PathManager mergedPosition = new PathManager();
        final Map<String, Object> property = new HashMap<String, Object>();
        property.put("handler", "JSONParser");
        final Parser jsonParser = ParserLoader.loadParser((Map)property, (UUID)null);
        final Iterator<Event> jsonNodeEventIterator = (Iterator<Event>)jsonParser.parse((InputStream)new ByteArrayInputStream(bytes));
        boolean isPositionAltered = false;
        try {
            while (jsonNodeEventIterator.hasNext()) {
                final JsonNodeEvent jsonNode = (JsonNodeEvent)jsonNodeEventIterator.next();
                final JsonNode position = jsonNode.data.findValue("position");
                if (position.isNull()) {
                    return null;
                }
                final ImmutableStemma positionStemma = KryoSingleton.readImmutableStemma(Base64.decodeBase64(position.toString()));
                final Position pos = positionStemma.toPosition();
                if (pos.containsComponent(targetUUID) || isPositionAltered) {
                    mergedPosition.mergeHigherPositions(pos);
                }
                else {
                    final Position alteredPosition = Utility.updateUuids(pos);
                    if (!alteredPosition.containsComponent(targetUUID)) {
                        return null;
                    }
                    mergedPosition.mergeHigherPositions(pos);
                    isPositionAltered = true;
                }
            }
        }
        catch (Exception e) {
            if (!(e instanceof RuntimeException)) {
                throw new RuntimeException("Unable to deserialize the message. Please check if the topic was filled with irrelevant data." + e);
            }
            final Throwable exp = e.getCause();
            if (!(exp instanceof RecordException) || ((RecordException)exp).type() != RecordException.Type.END_OF_DATASOURCE) {
                throw e;
            }
        }
        return mergedPosition.toPosition();
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}

