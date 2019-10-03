package com.datasphere.proc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.InvalidDataException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.event.Event;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.recovery.JSONSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.intf.BaseParser;
import com.datasphere.source.lib.intf.CheckpointProvider;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.utils.ReaderMetadataPersistenceUtility;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.JsonNodeEvent;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@PropertyTemplate(name = "JSONParser", type = AdapterType.parser, properties = {
		@PropertyTemplateProperty(name = "eventType", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "fieldName", type = String.class, required = false, defaultValue = "") }, inputType = JsonNodeEvent.class)
public class JSONParser_1_0 extends BaseParser implements CheckpointProvider, Observer {
	private static Logger logger;
	JsonFactory f;
	JsonParser jp;
	JsonNodeFactory jnf;
	Class<?> eventClass;
	ObjectMapper mapper;
	protected CheckpointDetail checkpoint;
	Reader reader;
	private List<String> unknownPropertyNameList;
	Property property;
	UUID srcUUID;
	List<Event> eventList;
	int count;
	Stack<JsonNode> nodeStack;
	String fromFieldName;
	private String distributionID;
	private boolean receivedInvalidRecord;
	private boolean firstEvent;
	private long eventTimestamp;
	private long eventCounter;

	public JSONParser_1_0(final Map<String, Object> prop, final UUID sourceUUID) throws AdapterException {
		super((Map) prop, sourceUUID);
		this.f = new JsonFactory();
		this.jp = null;
		this.jnf = JsonNodeFactory.instance;
		this.eventClass = null;
		this.mapper = ObjectMapperFactory.newInstance();
		this.unknownPropertyNameList = new LinkedList<String>();
		this.fromFieldName = null;
		this.receivedInvalidRecord = false;
		this.firstEvent = false;
		this.eventTimestamp = 0L;
		this.eventCounter = 0L;
		this.property = new Property((Map) prop);
		this.srcUUID = sourceUUID;
		this.distributionID = (String)prop.get("distributionId");
		if (JSONParser_1_0.logger.isTraceEnabled()) {
			JSONParser_1_0.logger.trace((Object) ("JSONParser is initialized with following properties\nEvent Type - ["
					+ this.property.getString("eventType", "") + "]"));
		}
	}

	public Event next() {
		if (this.count < this.eventList.size()) {
			final Event event = this.eventList.get(this.count++);
			this.eventTimestamp = event.getTimeStamp();
			if (!this.firstEvent) {
				ReaderMetadataPersistenceUtility.persistFirstEventTimestamp(this.reader, this.eventTimestamp);
				this.firstEvent = true;
			}
			++this.eventCounter;
			return event;
		}
		throw new RuntimeException("No data to return");
	}

	public void remove() {
	}

	public Iterator<Event> parse(final Reader in) throws Exception {
		this.reader = in;
		this.eventList = new ArrayList<Event>();
		final UnknownPropertyHandler handler = new UnknownPropertyHandler();
		this.mapper.getDeserializationConfig().withHandler((DeserializationProblemHandler) handler);
		this.nodeStack = new Stack<JsonNode>();
		this.reader.skipBytes(0L);
		final Object fromFieldNameVal = this.property.getString("fieldName", (String) null);
		if (fromFieldNameVal != null && !fromFieldNameVal.toString().isEmpty()) {
			this.fromFieldName = fromFieldNameVal.toString();
		}
		final String eventTypeName = this.property.getString("eventType", "");
		if (eventTypeName != null && !eventTypeName.isEmpty()) {
			String typeNamespace = null;
			String typeNameOnly = null;
			if (eventTypeName.indexOf(".") != -1) {
				typeNamespace = eventTypeName.split("\\.")[0];
				typeNameOnly = eventTypeName.split("\\.")[1];
			} else {
				typeNamespace = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.srcUUID,
						HDSecurityManager.TOKEN).nsName;
				typeNameOnly = eventTypeName;
			}
			try {
				final MetaInfo.Type eventType = (MetaInfo.Type) MetadataRepository.getINSTANCE().getMetaObjectByName(
						EntityType.TYPE, typeNamespace, typeNameOnly, (Integer) null, HDSecurityManager.TOKEN);
				if (eventType != null) {
					this.eventClass = ClassLoader.getSystemClassLoader().loadClass(eventType.className);
				} else {
					this.eventClass = ClassLoader.getSystemClassLoader().loadClass(eventTypeName);
				}
			} catch (ClassNotFoundException cnfe) {
				throw new AdapterException("Failure in JSONParser initialization. Could not locate type or class "
						+ eventTypeName + " to convert to from JSON");
			} catch (MetaDataRepositoryException e) {
				throw new AdapterException("Failure in JSONParser initialization", (Throwable) e);
			}
		}
		try {
			this.jp = this.f.createParser((InputStream) in);
		} catch (Exception e2) {
			if (e2 instanceof RuntimeException) {
				final Throwable exp = e2.getCause();
				if (exp instanceof RecordException
						&& ((RecordException) exp).type() == RecordException.Type.NO_RECORD) {
					throw new AdapterException(
							"Failure in JSONParser initialization. Unable to detect encoding because of insufficient or no data");
				}
			}
			throw new AdapterException("Could not initialize JSON Parser: ", (Throwable) e2);
		}
		this.reader.registerObserver((Object) this);
		return (Iterator<Event>) this;
	}

	public boolean hasNext() {
		try {
			if (this.count < this.eventList.size()) {
				return true;
			}
			this.eventList.clear();
			this.count = 0;
			JsonNode currNode = null;
			JsonNode parent = null;
			String currField = null;
			this.checkpoint = this.reader.getCheckpointDetail();
			int arrayCount = 0;
			int objectCount = 0;
			JsonToken token;
			while ((token = this.jp.nextToken()) != null) {
				if (!this.nodeStack.empty()) {
					parent = this.nodeStack.peek();
				}
				switch (token) {
				case START_OBJECT: {
					if (this.receivedInvalidRecord) {
						this.receivedInvalidRecord = false;
						objectCount = 0;
						arrayCount = 0;
					}
					currNode = (JsonNode) new ObjectNode(this.jnf);
					this.nodeStack.push(currNode);
					++objectCount;
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).set(currField, currNode);
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(currNode);
							continue;
						}
						continue;
					}
				}
				case START_ARRAY: {
					if (this.receivedInvalidRecord) {
						this.receivedInvalidRecord = false;
						arrayCount = 0;
						objectCount = 0;
					}
					currNode = (JsonNode) new ArrayNode(this.jnf);
					this.nodeStack.push(currNode);
					++arrayCount;
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).set(currField, currNode);
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(currNode);
							continue;
						}
						continue;
					}
				}
				case FIELD_NAME: {
					currField = this.jp.getCurrentName();
					continue;
				}
				case VALUE_EMBEDDED_OBJECT: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getText());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getText());
							continue;
						}
						continue;
					}
				}
				case VALUE_STRING: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getText());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getText());
							continue;
						}
						continue;
					}
				}
				case VALUE_TRUE: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getBooleanValue());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getBooleanValue());
							continue;
						}
						continue;
					}
				}
				case VALUE_FALSE: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getBooleanValue());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getBooleanValue());
							continue;
						}
						continue;
					}
				}
				case VALUE_NUMBER_FLOAT: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getDoubleValue());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getDoubleValue());
							continue;
						}
						continue;
					}
				}
				case VALUE_NUMBER_INT: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).put(currField, this.jp.getLongValue());
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).add(this.jp.getLongValue());
							continue;
						}
						continue;
					}
				}
				case VALUE_NULL: {
					if (parent instanceof ObjectNode) {
						assert currField != null;
						((ObjectNode) parent).putNull(currField);
						continue;
					} else {
						if (parent instanceof ArrayNode) {
							((ArrayNode) parent).addNull();
							continue;
						}
						continue;
					}
				}
				case END_ARRAY: {
					if (!this.nodeStack.isEmpty()) {
						currNode = this.nodeStack.pop();
					}
					currField = null;
					--arrayCount;
					if (objectCount == 0 && arrayCount == 0 && currNode != null) {
						this.outputEvents(currNode);
						return true;
					}
					continue;
				}
				case END_OBJECT: {
					if (!this.nodeStack.isEmpty()) {
						currNode = this.nodeStack.pop();
					}
					currField = null;
					if (--objectCount == 0 && arrayCount == 0 && currNode != null) {
						this.outputEvents(currNode);
						return true;
					}
					continue;
				}
				}
			}
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				final Throwable exp = e.getCause();
				if (exp instanceof RecordException
						&& ((RecordException) exp).type() == RecordException.Type.NO_RECORD) {
					return false;
				}
			} else if (e instanceof JsonParseException) {
				if (!this.receivedInvalidRecord) {
					this.receivedInvalidRecord = true;
					throw new RuntimeException((Throwable) new InvalidDataException((Throwable) e));
				}
				return false;
			}
			throw new RuntimeException(
					(Throwable) new RecordException("Could not parse " + e.getMessage() + "\nInvalid Json Record.",
							(Throwable) e, RecordException.Type.INVALID_RECORD));
		}
		throw new RuntimeException((Throwable) new RecordException(RecordException.Type.END_OF_DATASOURCE));
	}

	private void addEvent(final JsonNode node) throws Exception {
		if (this.eventClass == null) {
			final JsonNodeEvent eventOut = new JsonNodeEvent(System.currentTimeMillis());
			eventOut.metadata = this.reader.getEventMetadata();
			eventOut.data = node;
			this.eventList.add((Event) eventOut);
		} else {
			final Event out = (Event) this.mapper.treeToValue((TreeNode) node, (Class) this.eventClass);
			this.eventList.add(out);
		}
	}

	private void outputEvents(final JsonNode currNode) throws Exception {
		if (JSONParser_1_0.logger.isDebugEnabled()) {
			final ObjectMapper objMapper = new ObjectMapper();
			final Object obj = objMapper.treeToValue((TreeNode) currNode, (Class) Object.class);
			final String jsonData = objMapper.writeValueAsString(obj);
			JSONParser_1_0.logger.debug((Object) ("Parsed Json data : [" + jsonData + "]"));
		}
		JsonNode useNode = currNode;
		if (this.fromFieldName != null) {
			useNode = currNode.get(this.fromFieldName);
			if (useNode == null) {
				throw new RuntimeException(
						"Specified field " + this.fromFieldName + " does not exist in " + currNode.toString());
			}
		}
		if (useNode.isArray()) {
			for (int i = 0; i < useNode.size(); ++i) {
				final JsonNode child = useNode.get(i);
				this.addEvent(child);
			}
		} else {
			this.addEvent(useNode);
		}
		this.nodeStack.clear();
	}

	public void close() throws AdapterException {
		try {
			if (this.reader != null) {
				ReaderMetadataPersistenceUtility.persistEventCountAndLastEventTimestamp(this.reader, this.eventCounter,
						this.eventTimestamp);
				this.eventCounter = 0L;
				this.eventTimestamp = 0L;
				this.reader.close();
			}
			if (this.jp != null) {
				this.jp.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public CheckpointDetail getCheckpointDetail() {
		return this.checkpoint;
	}

	public static void main(final String[] args) throws Exception {
		final Map<String, Object> props = new HashMap<String, Object>();
		props.put("directory", "/home/arul/Workspace/repo/Product/IntegrationTests/TestData");
		props.put("wildcard", "simpleapp.json");
		props.put("positionByEOF", false);
		props.put("eventType", "");
		props.put("readerType", Reader.FILE_READER);
		props.put("breakonnorecord", true);
		final ObjectMapper mapper = ObjectMapperFactory.newInstance();
		final Reader fileReader = Reader.createInstance(new Property((Map) props));
		fileReader.position((CheckpointDetail) null, true);
		final JSONParser_1_0 json = new JSONParser_1_0(props, null);
		final Iterator<Event> itr = json.parse(fileReader);
		while (true) {
			if (itr.hasNext()) {
				final Event event = itr.next();
				final Field[] fs = event.getClass().getDeclaredFields();
				String value = "Foo: " + event.getClass().getSimpleName() + "{\n";
				for (final Field f : fs) {
					if (Modifier.isPublic(f.getModifiers())) {
						if (!"mapper".equals(f.getName())) {
							value = value + "  " + f.getName() + ": " + mapper.writeValueAsString(f.get(event)) + "\n";
						}
					}
				}
				value += "};";
				System.out.println(value);
			} else {
				System.out.println("Going to retry for more event");
				Thread.sleep(100L);
			}
		}
	}

	public Position getPositionDetail() {
		final SourcePosition sourcePosition = (SourcePosition) ((this.checkpoint != null)
				? new JSONSourcePosition(this.checkpoint)
				: null);
		return Position.from(this.srcUUID, this.distributionID, sourcePosition);
	}

	public void update(final Observable o, final Object arg) {
		switch ((Constant.eventType) arg) {
		case ON_CLOSE: {
			ReaderMetadataPersistenceUtility.persistEventCountAndLastEventTimestamp(this.reader, this.eventCounter,
					this.eventTimestamp);
			this.eventCounter = 0L;
			this.eventTimestamp = 0L;
			this.firstEvent = false;
			break;
		}
		}
	}

	static {
		JSONParser_1_0.logger = Logger.getLogger((Class) JSONParser_1_0.class);
	}

	class UnknownPropertyHandler extends DeserializationProblemHandler {
		public boolean handleUnknownProperty(final DeserializationContext ctxt, final JsonParser jp,
				final JsonDeserializer<?> deserializer, final Object bean, final String propertyName)
				throws IOException, JsonProcessingException {
			if (!JSONParser_1_0.this.unknownPropertyNameList.contains(propertyName)) {
				JSONParser_1_0.this.unknownPropertyNameList.add(propertyName);
				if (JSONParser_1_0.logger.isDebugEnabled()) {
					JSONParser_1_0.logger.debug((Object) ("Unrecognized field \"" + propertyName
							+ "\" encountered. Field has been ignored."));
				}
			}
			return true;
		}
	}
	
	class SecurityAccess {
		public void disopen() {
			
		}
    }
}
