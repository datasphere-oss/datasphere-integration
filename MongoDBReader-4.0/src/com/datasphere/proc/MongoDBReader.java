package com.datasphere.proc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.intf.EventSink;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.ComponentCheckpoint;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.recovery.MongoDBReaderSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.utils.StringUtils;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.uuid.UUID;
import com.datasphere.mongodbreader.utils.AuthType;
import com.datasphere.mongodbreader.utils.Mode;
import com.datasphere.mongodbreader.utils.ReadPreference;
import com.datasphere.mongodbreader.utils.Utilities;
import com.datasphere.proc.events.JsonNodeEvent;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

@PropertyTemplate(name = "MongoDBReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "connectionUrl", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "collections", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "excludeCollections", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "mode", type = Mode.class, required = false, defaultValue = "InitialLoad"),
		@PropertyTemplateProperty(name = "userName", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "password", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "authType", type = AuthType.class, required = false, defaultValue = "Default"),
		@PropertyTemplateProperty(name = "authDB", type = String.class, required = false, defaultValue = "admin"),
		@PropertyTemplateProperty(name = "readPreference", type = ReadPreference.class, required = false, defaultValue = "primaryPreferred"),
		@PropertyTemplateProperty(name = "startTimestamp", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "sslEnabled", type = Boolean.class, required = false, defaultValue = "false"),
		@PropertyTemplateProperty(name = "connectionRetryPolicy", type = String.class, required = false, defaultValue = "retryInterval=30, maxRetries=3") }, inputType = JsonNodeEvent.class, requiresFormatter = false)
public class MongoDBReader extends SourceProcess {
	private static Logger logger;
	private String connectionUrl;
	private String collections;
	private String excludeCollections;
	private String userName;
	private String password;
	private String authDB;
	private String componentName;
	ReadPreference readPreference;
	AuthType authType;
	private BsonTimestamp startTimestamp;
	private MongoClient client;
	private boolean doInitialLoad;
	private boolean captureCDC;
	private boolean initialized;
	private boolean initialLoadCompleted;
	private boolean sslEnabled;
	private boolean fullDocumentUpdateLookup;
	private CollectionWatcher watcher;
	private MongoDBReaderSourcePosition sourcePosition;
	private Flow flow;
	private MongoCredential credential;
	private int maxRetries;
	private int retryInterval;
	private Map<String, BsonTimestamp> CDCStartPositions;
	private Map<String, List<String>> collectionMap;

	public MongoDBReader() {
		this.credential = null;
	}

	public void init(final Map<String, Object> properties, final Map<String, Object> prop2, final UUID uuid,
			final String distributionId, final SourcePosition startPosition, final boolean sendPositions,
			final Flow flow) throws Exception {
		super.init((Map) properties, (Map) prop2, uuid, distributionId, startPosition, sendPositions, flow);
		this.initialized = false;
		this.sourceUUID = uuid;
		this.distributionID = distributionId;
		this.flow = flow;
		if (this.sourceUUID != null) {
			this.componentName = MetadataRepository.getINSTANCE()
					.getMetaObjectByUUID(this.sourceUUID, WASecurityManager.TOKEN).getFullName();
		}
		this.readProperties(properties);
		this.client = Utilities.connectClient(this.connectionUrl, this.credential, this.readPreference.toString(),
				this.sslEnabled);
		this.fillCollections();
		if (startPosition != null && startPosition instanceof MongoDBReaderSourcePosition) {
			final MongoDBReaderSourcePosition givenPosition = (MongoDBReaderSourcePosition) startPosition;
			this.startTimestamp = new BsonTimestamp((long) givenPosition.getTimestamp());
			this.initialLoadCompleted = givenPosition.getInitialLoadStatus();
			this.sourcePosition = new MongoDBReaderSourcePosition(givenPosition.getInitialLoadStatus(),
					this.startTimestamp.getValue());
			if (MongoDBReader.logger.isDebugEnabled()) {
				MongoDBReader.logger
						.debug((Object) ("Attempting to restart from last position " + this.sourcePosition));
			}
		} else if (sendPositions) {
			this.sourcePosition = new MongoDBReaderSourcePosition(false,
					(this.startTimestamp != null) ? this.startTimestamp.getValue() : new BsonTimestamp().getValue());
		}
	}

	public void initlocal(final Map<String, Object> properties) throws Exception {
		this.init(properties, new HashMap<String, Object>(), null, null, null, false, null);
	}

	private void readProperties(final Map<String, Object> properties) {
		final Property property = new Property((Map) properties);
		this.connectionUrl = property.getString("connectionUrl", (String) null);
		if (this.connectionUrl == null) {
			this.throwIllegalArgumentException("connectionUrl");
		}
		this.collections = property.getString("collections", (String) null);
		if (this.collections == null) {
			this.throwIllegalArgumentException("collections");
		}
		this.excludeCollections = property.getString("excludeCollections", (String) null);
		if (property.getObject("mode", (Object) null) == null) {
			this.throwIllegalArgumentException("mode");
		}
		final Mode mode = Mode.valueOf(property.getObject("mode", (Object) null).toString());
		this.doInitialLoad = mode.toString().equalsIgnoreCase("InitialLoad");
		this.captureCDC = mode.toString().equalsIgnoreCase("Incremental");
		if (!this.doInitialLoad && !this.captureCDC) {
			throw new IllegalArgumentException(
					"Failure in initializing MongoDBReader, Invalid mode. Allowed values : 1. InitialLoad, 2. Incremental");
		}
		if (property.getString("startTimestamp", (String) null) != null) {
			try {
				final DateTime startTime = DateTime.parse(property.getString("startTimestamp", (String) null))
						.withZone(DateTimeZone.UTC);
				final Long givenTimeInMillis = startTime.getMillis();
				this.startTimestamp = new BsonTimestamp((int) (givenTimeInMillis / 1000L),
						(int) (givenTimeInMillis % 1000L));
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"Failure in initializing MongoDBReader, Error while processing the given startTimestamp : "
								+ e);
			}
		}
		this.userName = property.getString("userName", (String) null);
		this.password = property.getPassword("password");
		this.authType = AuthType.valueOf((String)property.getObject("authType", (Object)"Default"));
		this.authDB = property.getString("authDB", "admin");
		this.readPreference = ReadPreference
				.valueOf(property.getObject("readPreference", (Object) "primaryPreferred").toString());
		this.credential = Utilities.validateAuth(this.authType.toString(), this.authDB, this.userName, this.password);
		this.sslEnabled = property.getBoolean("sslEnabled", false);
		final String connectionRetryPolicy = property.getString("connectionRetryPolicy", "").isEmpty()
				? "retryInterval=30, maxRetries=3"
				: property.getString("connectionRetryPolicy", "");
		final Map<String, Object> connectionRetryPolicyMap = (Map<String, Object>) StringUtils
				.getKeyValuePairs(connectionRetryPolicy, ",", "=");
		if (connectionRetryPolicyMap.get("maxRetries") != null) {
			this.maxRetries = StringUtils.getInt((Map) connectionRetryPolicyMap, "maxRetries");
			if (this.maxRetries < 1) {
				this.maxRetries = 0;
			}
		}
		if (connectionRetryPolicyMap.get("retryInterval") != null) {
			this.retryInterval = StringUtils.getInt((Map) connectionRetryPolicyMap, "retryInterval");
			if (this.retryInterval < 1) {
				this.retryInterval = 0;
			}
		}
	}

	private void throwIllegalArgumentException(final String propertyName) {
		throw new IllegalArgumentException(
				"Failure in initializing MongoDBReader, " + propertyName + " property cannot be null");
	}

	private void fillCollections() throws Exception {
		final String[] includeList = this.collections.split(",");
		final Map<String, String> includeMap = this.getCollectionMap(includeList);
		Map<String, String> excludeMap = new HashMap<String, String>();
		if (this.excludeCollections != null && !this.excludeCollections.trim().isEmpty()) {
			final String[] excludeList = this.excludeCollections.split(",");
			excludeMap = this.getCollectionMap(excludeList);
		}
		this.collectionMap = new HashMap<String, List<String>>();
		Boolean foundOne = false;
		for (final String dbName : includeMap.keySet()) {
			final Set<String> collSet = Utilities.getCollectionSet(Utilities.connectDb(this.client, dbName),
					includeMap.get(dbName), excludeMap.get(dbName));
			if (!collSet.isEmpty()) {
				this.collectionMap.put(dbName, new ArrayList<String>(collSet));
				foundOne = true;
			}
		}
		if (!foundOne) {
			throw new IllegalArgumentException(
					"Given collections combination resulted in an empty list. Provide valid existing collections.");
		}
	}

	private Map<String, String> getCollectionMap(final String[] list) {
		final Map<String, String> map = new HashMap<String, String>();
		for (final String coll : list) {
			final int index = coll.indexOf(".");
			if (index < 0 || index >= coll.length()) {
				throw new IllegalArgumentException(
						"Collection name must be fully qualified [DatabaseName.CollectionName]");
			}
			final String dbName = coll.substring(0, index);
			String collName = coll.substring(index + 1, coll.length());
			if (map.get(dbName) != null) {
				collName = map.get(dbName) + "," + collName;
			}
			map.put(dbName, collName);
		}
		return map;
	}

	private void initialLoad() {
		final InitialLoader loader = new InitialLoader(this.client, this.collectionMap, this.sendCallback(),
				this.maxRetries, this.retryInterval, this.captureCDC);
		this.CDCStartPositions = loader.start();
		if (this.sourcePosition != null) {
			this.sourcePosition.setInitialLoadStatus(true);
		}
		if (!this.captureCDC) {
			this.client.close();
		}
	}

	private void watchCollections() {
		final List<String> collectionList = new ArrayList<String>();
		for (final Map.Entry<String, List<String>> entry : this.collectionMap.entrySet()) {
			final List<String> list = new ArrayList<String>(entry.getValue());
			list.replaceAll(collName -> entry.getKey() + "." + collName);
			collectionList.addAll(list);
		}
		(this.watcher = new CollectionWatcher(this.client, collectionList, this.startTimestamp, this.sendCallback(),
				this.maxRetries, this.retryInterval, this.CDCStartPositions, this.fullDocumentUpdateLookup))
						.setUncaughtExceptionHandler(this.getExceptionHandler());
		this.watcher.start();
	}

	private Consumer<JsonNodeEvent> sendCallback() {
		final Consumer<JsonNodeEvent> sendWAEvent = jne -> this.sendWAEvent(jne);
		return sendWAEvent;
	}

	private void sendWAEvent(final JsonNodeEvent jne) {
		if (jne != null) {
			try {
				if (this.sourcePosition != null) {
					if (this.watcher != null) {
						this.sourcePosition.setTimestamp(this.watcher.getTimestamp().getValue());
					}
					this.send((Event) jne, 0,
							(ImmutableStemma) ImmutableStemma.from(this.sourceUUID, this.distributionID,
									(SourcePosition) this.sourcePosition, (SourcePosition) this.sourcePosition, "@"));
				} else {
					this.send((Event) jne, 0);
				}
			} catch (Exception e) {
				final RuntimeException ex = new RuntimeException(
						"There was an exception while sending the event :" + e);
			}
		}
	}

	private Thread.UncaughtExceptionHandler getExceptionHandler() {
		return new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(final Thread th, final Throwable ex) {
				MongoDBReader.this.flow.notifyAppMgr(EntityType.SOURCE, MongoDBReader.this.componentName,
						MongoDBReader.this.sourceUUID, (Exception) new RuntimeException(th.getName() + " " + ex),
						"Exception in MongoDBReader while reading data", new Object[0]);
			}
		};
	}

	public Position getCheckpoint() {
		ImmutableStemma immutableStemma = null;
		if (this.sourcePosition == null) {
			this.sourcePosition = new MongoDBReaderSourcePosition(false,
					(this.startTimestamp != null) ? this.startTimestamp.getValue() : new BsonTimestamp().getValue());
		}
		if (this.sourceUUID != null) {
			immutableStemma = (ImmutableStemma) ImmutableStemma.from(this.sourceUUID, this.distributionID,
					(SourcePosition) this.sourcePosition, (SourcePosition) this.sourcePosition, "^");
		}
		if (MongoDBReader.logger.isDebugEnabled()) {
			MongoDBReader.logger.debug((Object) ("Recovery position getting saved: " + this.sourcePosition));
		}
		return (immutableStemma == null) ? null
				: immutableStemma.toPosition();
	}

	public void receiveImpl(final int channel, final Event event) throws Exception {
		if (!this.initialized) {
			if (this.doInitialLoad && !this.initialLoadCompleted) {
				this.initialLoad();
			}
			if (this.captureCDC) {
				this.watchCollections();
			}
			this.initialized = true;
		} else {
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException ex) {
			}
		}
	}

	public void close() throws Exception {
		if (this.watcher != null) {
			this.watcher.stopRunning();
			this.watcher.interrupt();
			this.watcher.join();
		}
		if (this.client != null) {
			this.client.close();
		}
	}

	public static void main(final String[] args) {
//		final Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
//		mongoLogger.setLevel(Level.ERROR);
//		final MongoDBReader writer = new MongoDBReader();
//		final Map<String, Object> props = new HashMap<String, Object>();
//		props.put("connectionUrl", "localhost:27017");
//		props.put("collections", "test.$");
//		props.put("excludeCollections", "test.a$");
//		props.put("mode", "Incremental");
//		props.put("AuthType", "NoAuth");
//		props.put("authDB", "admin");
//		props.put("userName", "test");
//		props.put("password", "test");
//		props.put("readPreference", "secondaryPreferred");
//		props.put("connectionRetryPolicy", "retryInterval=1, maxRetries=3");
//		props.put("sslEnabled", false);
//		writer.addEventSink((EventSink) new EventSink() {
//			private void output(final Event event) {
//				System.out.println(event);
//			}
//
//			public void receive(final int channel, final Event event) throws Exception {
//				this.output(event);
//			}
//
//			public void receive(final int channel, final Event event, final ImmutableStemma pos) throws Exception {
//				this.output(event);
//			}
//
//			public void receive(final ITaskEvent batch) throws Exception {
//			}
//
//			public UUID getNodeID() {
//				return null;
//			}
//		});
//		try {
//			writer.initlocal(props);
//			while (true) {
//				writer.receive(0, (Event) null);
//			}
//		} catch (Exception e) {
//			MongoDBReader.logger.error((Object) ("*** Problem with reader: " + e.getMessage()), (Throwable) e);
//		}
	}

	static {
		MongoDBReader.logger = Logger.getLogger((Class) MongoDBReader.class);
	}
}
