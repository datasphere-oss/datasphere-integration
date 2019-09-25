package com.datasphere.databasewritermgr;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Observer;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.exc.ConnectionException;
import com.datasphere.common.exc.InvalidDataException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.Utils;
import com.datasphere.uuid.UUID;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.Table.Table;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.TypeHandler.TypeHandler;
import com.datasphere.databasewriter.DatabaseWriter;
import com.datasphere.databasewriter.DatabaseWriterProcessEvent;
import com.datasphere.databasewriter.WildCardProcessor;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.proc.events.HDEvent;

public class DatabaseWriterMgr implements DatabaseWriter {
	private static Logger logger;
	private String dbUrl;
	private String dbUser;
	private String dbPasswd;
	private String tableList;
	private String excludeTableList;
	private String targetType;
	private DatabaseWriterConnection connectionObject;
	private DatabaseWriterProcessEvent eventProcessor;
	private WildCardProcessor wCardProcessor;
	private Observer observer;
	private final String CONNECTION_CLOSED = "08003";
	private final String NO_MORE_DATA = "08000";
	private final String TABLE_NAME = "TableName";
	private final String OPERATION_NAME = "OperationName";
	private final String OPERATION_VALUE = "INSERT";
	private final String PRESERVE_TXN_BOUNDARY = "PreserveSourceTransactionBoundary";
	private UUID typeUUID;
	private HashMap<String, Object> metadata;
	private boolean incomingEventIsTyped;
	private boolean preserveTxnBoundary;
	private Map<String, String> noTargetWarnMap;
	TableToTypeHandlerMap tableToTypeHandlerMap;

	public DatabaseWriterMgr() {
		this.dbUrl = null;
		this.dbUser = null;
		this.dbPasswd = null;
		this.tableList = null;
		this.excludeTableList = null;
		this.targetType = null;
		this.connectionObject = null;
		this.eventProcessor = null;
		this.wCardProcessor = null;
		this.typeUUID = null;
		this.metadata = null;
		this.incomingEventIsTyped = false;
		this.preserveTxnBoundary = false;
		this.noTargetWarnMap = new HashMap<String, String>();
	}

	private void validateInputParameters(final Map<String, Object> properties) throws Exception {
		try {
			if (!properties.containsKey("ConnectionURL") || properties.get("ConnectionURL") == null
					|| ((String) properties.get("ConnectionURL")).length() <= 0) {
				final DatabaseWriterException exception = new DatabaseWriterException(Error.CONNECTIONURL_NOT_SPECIFIED,
						"");
				DatabaseWriterMgr.logger.error(exception.getMessage());
				throw exception;
			}
			this.dbUrl = (String) properties.get("ConnectionURL");
		} catch (ClassCastException e) {
			final DatabaseWriterException exception2 = new DatabaseWriterException(Error.INVALID_CONNECTIONURL_FORMAT,
					"");
			DatabaseWriterMgr.logger.error(exception2.getMessage());
			throw exception2;
		}
		try {
			if (!properties.containsKey("Username") || properties.get("Username") == null
					|| ((String) properties.get("Username")).length() <= 0) {
				final DatabaseWriterException exception = new DatabaseWriterException(Error.USERNAME_NOT_SPECIFIED, "");
				DatabaseWriterMgr.logger.error(exception.getMessage());
				throw exception;
			}
			this.dbUser = (String) properties.get("Username");
		} catch (ClassCastException e) {
			final DatabaseWriterException exception2 = new DatabaseWriterException(Error.INVALID_USERNAME_FORMAT, "");
			DatabaseWriterMgr.logger.error(exception2.getMessage());
			throw exception2;
		}
		try {
			if (!properties.containsKey("Password") || properties.get("Password") == null
					|| ((String) properties.get("Password")).length() <= 0) {
				final DatabaseWriterException exception = new DatabaseWriterException(Error.PASSWORD_NOT_SPECIFIED, "");
				DatabaseWriterMgr.logger.error(exception.getMessage());
				throw exception;
			}
			this.dbPasswd = (String) properties.get("Password");
		} catch (ClassCastException e) {
			final DatabaseWriterException exception2 = new DatabaseWriterException(Error.INVALID_PASSWORD_FORMAT, "");
			DatabaseWriterMgr.logger.error(exception2.getMessage());
			throw exception2;
		}
		final Property prop = new Property(properties);
		this.tableList = prop.getString("Maps", (String) null);
		if (this.tableList == null) {
			this.tableList = prop.getString("Tables", (String) null);
		}
		if (this.tableList == null) {
			final DatabaseWriterException exception2 = new DatabaseWriterException(Error.TABLES_NOT_SPECIFIED, "");
			DatabaseWriterMgr.logger.error(exception2.getMessage());
			throw exception2;
		}
		try {
			if (properties.containsKey("ExcludedTables") && properties.get("ExcludedTables") != null
					&& ((String) properties.get("ExcludedTables")).length() > 0) {
				this.excludeTableList = (String) properties.get("ExcludedTables");
			}
		} catch (ClassCastException e2) {
			final DatabaseWriterException exception3 = new DatabaseWriterException(Error.INVALID_TABLES_FORMAT, "");
			DatabaseWriterMgr.logger.error(exception3.getMessage());
			throw exception3;
		}
		this.targetType = Utils.getDBType(this.dbUrl);
		if (this.targetType.isEmpty()) {
			throw new DatabaseWriterException(Error.INVALID_CONNECTIONURL_FORMAT,
					"Invalid URL format {" + this.dbUrl + "}");
		}
	}

	@Override
	public synchronized void initDatabaseWriter(final Map<String, Object> localCopyOfProperty,
			final Map<String, Object> formatterProperties, final UUID inputStream, final String distributionID)
			throws Exception {
		final Map<String, Object> properties = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
		properties.putAll(localCopyOfProperty);
		if (properties.get("Password") != null) {
			final String pp = ((Password) properties.get("Password")).getPlain();
			properties.put("Password", pp);
		}
		if (this.dbUrl.startsWith("jdbc:mysql:")) {
			if (this.dbUrl.indexOf("useCursorFetch=true") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?useCursorFetch=true";
				} else {
					this.dbUrl += "&useCursorFetch=true";
				}
			}
			if (this.dbUrl.indexOf("useSSL=") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?useSSL=false";
				} else {
					this.dbUrl += "&useSSL=false";
				}
			}
			if (this.dbUrl.indexOf("serverTimezone=") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?serverTimezone=GMT";
				} else {
					this.dbUrl += "&serverTimezone=GMT";
				}
			}
			if (this.dbUrl.indexOf("autoReconnect=") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?autoReconnect=true";
				} else {
					this.dbUrl += "&autoReconnect=true";
				}
			}
			if (this.dbUrl.indexOf("failOverReadOnly=") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?failOverReadOnly=false";
				} else {
					this.dbUrl += "&failOverReadOnly=false";
				}
			}
			if (this.dbUrl.indexOf("maxReconnects=") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?maxReconnects=1000";
				} else {
					this.dbUrl += "&maxReconnects=1000";
				}
			}
			if (this.dbUrl.indexOf("initialTimeout") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?initialTimeout=30";
				} else {
					this.dbUrl += "&initialTimeout=30";
				}
			}
		}else if (this.dbUrl.startsWith("jdbc:postgresql:")) {
			if (this.dbUrl.indexOf("stringtype") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?stringtype=unspecified";
				} else {
					this.dbUrl += "&stringtype=unspecified";
				}
			}
			if (this.dbUrl.indexOf("tcpKeepAlive") < 0) {
				if (this.dbUrl.indexOf("?") < 0) {
					this.dbUrl += "?tcpKeepAlive=true";
				} else {
					this.dbUrl += "&tcpKeepAlive=true";
				}
			}
		}
		this.validateInputParameters(properties);
		if (inputStream != null) {
			final MetaInfo.Stream stream = (MetaInfo.Stream) MetadataRepository.getINSTANCE()
					.getMetaObjectByUUID(inputStream, HDSecurityManager.TOKEN);
			final MetaInfo.Type dataType = (MetaInfo.Type) MetadataRepository.getINSTANCE()
					.getMetaObjectByUUID(stream.dataType, HDSecurityManager.TOKEN);
			if (!dataType.className.equals("com.datasphere.proc.events.HDEvent") || this.tableList.indexOf(",") < 0) {
				// if (!dataType.className.equals("com.datasphere.proc.events.HDEvent")) {
				this.incomingEventIsTyped = true;
				this.typeUUID = dataType.uuid;
				(this.metadata = new HashMap<String, Object>()).put("TableName", dataType.name);
				this.metadata.put("OperationName", "INSERT");
				if (this.tableList.indexOf(",") < 0) {
					this.tableList = dataType.name + "," + this.tableList;
				} else {
					this.tableList = dataType.name + "," + this.tableList.split(",")[1];
				}
			}
		}
		final Property prop = new Property(properties);
		this.preserveTxnBoundary = prop.getBoolean("PreserveSourceTransactionBoundary", false);
		if (this.preserveTxnBoundary) {
			final Map<String, Object> tmpMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
			tmpMap.putAll(properties);
			tmpMap.put("CommitPolicy", "EventCount:0,Interval:0");
			tmpMap.put("BatchPolicy", "EventCount:0,Interval:0");
		}
		this.wCardProcessor = new WildCardProcessor();
		this.connectionObject = DatabaseWriterConnection.getConnection(this.dbUrl, this.dbUser, this.dbPasswd);
		final String[] includeTablesArray = this.tableList.split(";");
		String[] excludeTablesArray = null;
		if (this.excludeTableList != null) {
			excludeTablesArray = this.excludeTableList.split(";");
		}

		this.wCardProcessor.initializeWildCardProcessor(includeTablesArray, excludeTablesArray);
		this.eventProcessor = new DatabaseWriterProcessEvent(this.connectionObject, this.wCardProcessor, prop);
		this.eventProcessor.setSourceUUID(inputStream);// 添加
		this.tableToTypeHandlerMap = this.eventProcessor.getTableToTypeHandlerMap();
		if (this.observer != null) {
			this.eventProcessor.addObserver(this.observer);
			this.observer = null;
		}
		if (this.preserveTxnBoundary) {
			this.eventProcessor.setAutoCommit(false);
			this.eventProcessor.honurTxnBoundary(true);
		}
	}

	private Event convertTypedEventToHDEvent(final Event event) {
		HDEvent HDEvent = null;
		if (event instanceof HDEvent) {
			HDEvent evt = (HDEvent) event;
			if (evt.typeUUID == null) {
				evt.typeUUID = this.typeUUID;
				evt.metadata = this.metadata;
			}
			return event;
		} else {
			try {
				JSONObject object = new JSONObject(event.toString());
				if (object.isNull("data")) {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						HDEvent = new HDEvent(payloadLength, (UUID) null);
						HDEvent.metadata = this.metadata;
						HDEvent.typeUUID = this.typeUUID;
						HDEvent.data = new Object[payloadLength];
						int i = 0;
						for (final Object o : payload) {
							HDEvent.setData(i++, o);
						}
					}
				} else {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						HDEvent = new HDEvent(payloadLength, (UUID) null);
						HDEvent.metadata = this.metadata;
						HDEvent.typeUUID = this.typeUUID;
						HDEvent.data = new Object[payloadLength];
						int i = 0;
						for (final Object o : payload) {
							HDEvent.setData(i++, o);
						}
					} else {
						JSONArray objs = (JSONArray) object.get("data");
						int payloadLength = objs.length();
						HDEvent = new HDEvent(payloadLength, (UUID) null);
						HDEvent.metadata = this.metadata;
						HDEvent.typeUUID = this.typeUUID;
						HDEvent.data = new Object[payloadLength];
						for (int i = 0; i < objs.length(); i++) {
							Object obj = (Object) objs.get(i);
							HDEvent.setData(i, obj);
						}
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
		return (Event) HDEvent;
	}

	@Override
	public void processEvent(final int channel, Event event, final Position position) throws Exception {
		if (this.incomingEventIsTyped) {
			event = this.convertTypedEventToHDEvent(event);
		}

//		System.out.println("----接收到消息000：" + event.toString());
		final String sourceType = (String) ((HDEvent) event).metadata.get("SourceType");

		String sourceTable = (String) ((HDEvent) event).metadata.get("TableName");
		if ("dm dbms".equalsIgnoreCase(sourceType)) {
			
		}
		if (sourceTable != null) {
			final ArrayList<String> targetTables = this.wCardProcessor.getMapForSourceTable(sourceTable);
			if (targetTables == null || targetTables.isEmpty()) {
				if (!this.noTargetWarnMap.containsKey(sourceTable)) {
					this.noTargetWarnMap.put(sourceTable, sourceTable);
					if (DatabaseWriterMgr.logger.isInfoEnabled()) {
						DatabaseWriterMgr.logger.info("No target found for {" + sourceTable + "}, ignoring the event");
					}
				}
				return;
			}
			boolean hasTarget = false;
			for (final String tableName : targetTables) {
				final String key = TableToTypeHandlerMap.formKey(tableName, (HDEvent) event);
				final TypeHandler[] tblColumnMapping = this.tableToTypeHandlerMap.getColumnMapping(key);
				if (tblColumnMapping == null) {
					try {
						final Table targetTableDef = (Table) this.eventProcessor.getTableMap().getTableMeta(tableName);
						if (targetTableDef == null) {
							continue;
						}

						this.tableToTypeHandlerMap.initializeForTable(key, targetTableDef);
						final String fullyQualifiedKey = TableToTypeHandlerMap
								.formKey(targetTableDef.getFullyQualifiedName(), (HDEvent) event);
						this.tableToTypeHandlerMap.initializeForTable(fullyQualifiedKey, targetTableDef);

						if (((HDEvent) event).data != null) {
							final int expectedColumnCount = ((HDEvent) event).data.length;
							final int tblColCnt = targetTableDef.getColumnCount();
							if (expectedColumnCount > tblColCnt) {
								final String errMsg = "Target {" + tableName
										+ "} has less number of column as compared to source {" + sourceTable
										+ "}.No of columns in target {" + tblColCnt + "} but expected {"
										+ expectedColumnCount + "}";
								throw new DatabaseWriterException(Error.INCONSISTENT_TABLE_STRUCTURE, errMsg);
							}
						}

						hasTarget = true;
					} catch (DatabaseWriterException exp) {
						if (exp.getErrorCode() != Error.TARGET_TABLE_DOESNOT_EXISTS.getType()) {
							throw exp;
						}
						continue;
					}
				} else {
					hasTarget = true;
				}
			}
			if (!hasTarget) {
				if (!this.noTargetWarnMap.containsKey(sourceTable)) {
					this.noTargetWarnMap.put(sourceTable, sourceTable);
					if (DatabaseWriterMgr.logger.isInfoEnabled()) {
						DatabaseWriterMgr.logger.info("No target found for {" + sourceTable + "}, ignoring the event");
					}
				}
				return;
			}
		}
		try {
			if (this.eventProcessor != null) {
//				System.out.println("---进入处理");
				this.eventProcessor.processEvents(channel, event, position);
			}
		} catch (DatabaseWriterException exp2) {
			exp2.printStackTrace();
			final Throwable cause = exp2.getCause();
			if (cause instanceof SQLException) {
				final String sqlErroCode = ((SQLException) cause).getSQLState();
				if ("08003".equals(sqlErroCode) || "08000".equals(sqlErroCode)) {
					throw new ConnectionException("URL {" + this.dbUrl + "} Connection is closed", (Throwable) exp2);
				}
			} else if (exp2.getErrorCode() == Error.MISSING_BEFORE_IMG.getType()) {
				throw new InvalidDataException(exp2.getErrorMessage(), (Throwable) exp2);
			}
			throw exp2;
		}
	}

	@Override
	public synchronized void closeDatabaseWriter() throws Exception {
		if (this.eventProcessor != null) {
			this.eventProcessor.endProcessing();
		}
	}

	@Override
	public void registerObserver(final Observer observer) {
		if (this.eventProcessor != null) {
			this.eventProcessor.addObserver(observer);
		} else {
			this.observer = observer;
		}
	}

	@Override
	public Position getWaitPositionFromWriter(final String key) throws DatabaseWriterException {
		Position waitPosition = null;
		if (this.eventProcessor != null) {
			waitPosition = this.eventProcessor.getWaitPositionFromWriter(key);
		}
		return waitPosition;
	}

	@Override
	public DateTime lastCommitTime() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.lastCommitTime();
		}
		return null;
	}

	@Override
	public long eventCount() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.eventCount();
		}
		return 0L;
	}

	@Override
	public long executionLatency() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.executionLatency();
		}
		return 0L;
	}

	@Override
	public long batchedEventCount() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.batchedEventCount();
		}
		return 0L;
	}

	@Override
	public DateTime lastBatcheExecuteTime() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.lastBatchExecuteTime();
		}
		return null;
	}

	@Override
	public long eventsInLastCommit() {
		if (this.eventProcessor != null) {
			return this.eventProcessor.eventsInLastCommit();
		}
		return 0L;
	}

	static {
		DatabaseWriterMgr.logger = LoggerFactory.getLogger(DatabaseWriterMgr.class);
	}

	public static void main(String[] arg) {
		DatabaseWriterMgr mgr = new DatabaseWriterMgr();
		mgr.wCardProcessor = new WildCardProcessor();
		String[] includeTablesArray = "BUFFER_TEST.TEST_DT,public.TEST;".split(";");

		try {
			mgr.wCardProcessor.initializeWildCardProcessor(includeTablesArray, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<String> targetTables = mgr.wCardProcessor.getMapForSourceTable("BUFFER_TEST.test_dt1");
		System.out.println(targetTables);
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
}
