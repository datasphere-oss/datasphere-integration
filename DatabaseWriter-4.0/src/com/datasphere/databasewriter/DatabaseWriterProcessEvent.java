package com.datasphere.databasewriter;

import java.io.File;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.appmanager.AppManager;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.meta.MetaInfo.Target;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.cdc.common.DatabaseTable;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.uuid.UUID;
import com.datasphere.Checkpoint.CheckpointTableImpl;
import com.datasphere.Connection.DatabaseWriterConnection;
import com.datasphere.OperationHandler.AlterIndexHandler;
import com.datasphere.OperationHandler.AlterTableHandler;
import com.datasphere.OperationHandler.AlterViewHandler;
import com.datasphere.OperationHandler.AnalyzeHandler;
import com.datasphere.OperationHandler.CTASHandler;
import com.datasphere.OperationHandler.CreateFunctionHandler;
import com.datasphere.OperationHandler.CreateIndexHandler;
import com.datasphere.OperationHandler.CreatePackageHandler;
import com.datasphere.OperationHandler.CreateProcedureHandler;
import com.datasphere.OperationHandler.CreateSequenceHandler;
import com.datasphere.OperationHandler.CreateTableHandler;
import com.datasphere.OperationHandler.CreateViewHandler;
import com.datasphere.OperationHandler.DDLHandler;
import com.datasphere.OperationHandler.DeleteHandler;
import com.datasphere.OperationHandler.DropObjectHandler;
import com.datasphere.OperationHandler.GrantHandler;
import com.datasphere.OperationHandler.InsertHandler;
import com.datasphere.OperationHandler.OperationHandler;
import com.datasphere.OperationHandler.PassThroughDDLHandler;
import com.datasphere.OperationHandler.PkUpdateHandler;
import com.datasphere.OperationHandler.RenameTableHandler;
import com.datasphere.OperationHandler.TruncateTableHandler;
import com.datasphere.OperationHandler.UpdateHandler;
import com.datasphere.Policy.ExecutionPolicy;
import com.datasphere.Policy.Policy;
import com.datasphere.Policy.PolicyCallback;
import com.datasphere.Policy.PolicyFactory;
import com.datasphere.Table.SourceTableMap;
import com.datasphere.Table.Table;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.exception.OperationNotSupported;
import com.datasphere.intf.DBInterface;
import com.datasphere.parser.SqlParser;
import com.datasphere.proc.common.AppStatus;
import com.datasphere.proc.entity.EventLog;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.proc.utils.PostgresqlUtils;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;

public class DatabaseWriterProcessEvent extends Observable implements DBInterface, PolicyCallback {
	private static String[] entityTypes;
	private final String PK_UPDATE = "PK_UPDATE";
	private final String APPLY_DDL = "ApplyDDL";
	private Logger logger;
	private DatabaseWriterConnection connectionObject;
	private boolean commitSupported;
	private boolean supportTxnBoundary;
	private Savepoint savePoint;
	// private final String BooleanTrue = "1";
	public Exception lastException;
	private Map<String, PreparedStatement> preparedStmtCache;
	private String chkPntTblKey;
	private Policy policy;
	private PathManager consolidatedPosition;
	private Event causedEvent;
	private boolean applyDDL;
	private TargetTableMap tableMap;
	private UUID targetUid;
	private Map<String, OperationHandler> handlerMap;
	private String currentSchema;
	private String currentCatalog;
	private boolean checkForSchema;
	private OperationHandler[] dmlHandlers;
	private WildCardProcessor wildcardProcessor;
	private TGSqlParser sqlParser;
	private DateTime lastCommitTime;
	private DateTime lastBatchExecuteTime;
	private long rowCount;
	private CheckpointTableImpl chkPntTblImpl;
	private long executionLatency;
	private long batchedEventCount;
	private long eventsInLastCommit;
	private boolean possiblePendingDDL;
	private long tmpCommitEventCount;
	private ExecutionPolicy executionPolicy;
	private TableToTypeHandlerMap tableToTypeHandlerMap;
	private String[] tables = null;// 源表和目标表
	private ScheduledThreadPoolExecutor pool = null;
	private UUID sourceUUID = null;
	private UUID appUUID = null;
	private String appName = null;
	private int commitCount = 0;// 提交记数
	private long batchCount = 0l;
	private boolean isCommited = true;
	private PostgresqlUtils exceptionLogUtils;
	private EventLog eventLog;
	private String batchUUID;
	private boolean updating = false;
	private int totalDataCount = 0;

	public DatabaseWriterProcessEvent(final DatabaseWriterConnection connectionObject,
			final WildCardProcessor wildcardProcessor, final Property prop) throws DatabaseWriterException {
		this.logger = LoggerFactory.getLogger(DatabaseWriterProcessEvent.class);
		this.commitSupported = true;
		this.supportTxnBoundary = false;
		this.lastCommitTime = null;
		this.lastBatchExecuteTime = null;
		this.chkPntTblImpl = null;
		this.executionLatency = 0L;
		this.batchedEventCount = 0L;
		this.eventsInLastCommit = 0L;
		this.possiblePendingDDL = true;
		this.tmpCommitEventCount = 0L;
		this.connectionObject = connectionObject;
		this.wildcardProcessor = wildcardProcessor;
		this.rowCount = 0L;
		this.exceptionLogUtils = new PostgresqlUtils();
		this.eventLog = new EventLog();
		this.init(prop);
	}

	public void setSourceUUID(UUID uuid) {
		this.sourceUUID = uuid;
	}

	public UUID getSourceUUID() {
		return this.sourceUUID;
	}

	protected void init(final Property prop) throws DatabaseWriterException {
		if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(this.connectionObject.targetType())
				|| Constant.GREENPLUM_TYPE.equalsIgnoreCase(this.connectionObject.targetType())
				|| Constant.HIVE_TYPE.equalsIgnoreCase(this.connectionObject.targetType())) {
			this.commitSupported = false;
		}
		if (Constant.ORACLE_TYPE.equalsIgnoreCase(this.connectionObject.targetType())) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Enabling DDL support.");
			}
			this.applyDDL = true;
		}
		this.setAutoCommit(false);
//		System.out.println("----目标数据库类型：" + this.connectionObject.targetType());
		this.targetUid = this.getUUID(prop.propMap, "TargetUUID");
		this.tables = getTables(prop.propMap);
		prop.propMap.put("TargetUUID", this.targetUid.getUUIDString());
		this.preparedStmtCache = new HashMap<String, PreparedStatement>();
		this.currentSchema = this.connectionObject.schema();
		this.currentCatalog = this.connectionObject.catalog();

		try {
			Target current_stream = (Target) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUid,
					HDSecurityManager.TOKEN);
			this.appUUID = current_stream.getCurrentApp().getUuid();
			this.appName = current_stream.getCurrentApp().getName();
		} catch (MetaDataRepositoryException e) {
			e.printStackTrace();
		}

		this.initializeSQLParser();
		this.initializeTableMap();
		this.initializeTypeMap();

		this.enableReadWrite();
		this.initializeHandler();
		this.initializeCheckpointTable(prop, this.connectionObject);
		this.policy = PolicyFactory.createPolicy(prop, this, this);
		this.executionPolicy = ExecutionPolicy.createPolicy(prop, this);
		
		if (this.appUUID != null) {
			batchUUID = getMD5(System.currentTimeMillis() + "");
			eventLog.setBatch_uuid(batchUUID);
			eventLog.setApp_uuid(this.appUUID.getUUIDString());
			eventLog.setApp_name(this.appName);
			eventLog.setCluster_name(HazelcastSingleton.getClusterName());
			eventLog.setNode_id(HazelcastSingleton.getBindingInterface());
			eventLog.setApp_progress("0%");
		}
		updating = true;

		autoCommit();
	}

	/**
	 * 启用事务可读写（postgresql）
	 */
	private void enableReadWrite() {
		if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(this.connectionObject.targetType())) {
			Statement stat = null;
			try {
				stat = this.connectionObject.getConnection().createStatement();
				stat.execute("set transaction read write");
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (stat != null) {
						stat.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 定时自动提交
	 */
	private void autoCommit() {
		ThreadFactory namedThreadFactory = (Runnable r) -> new Thread(r, "thread_pool_DatabaseWriter_" + r.hashCode());
		pool = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
		pool.scheduleAtFixedRate(() -> {
			try {
				if (commitCount > 2 && isCommited) {
					commitCount = 0;
					commit();
				}
				commitCount++;
//				if (updating && eventLog.getEvent_starttime() != null) {
//					updating = false;
//					if (this.appUUID != null) {
//						exceptionLogUtils.insertOrUpdate(eventLog); /** 记录数据Event的日志 和 数据交换日志 */
//					}
//					updating = true;
//				}
			} catch (DatabaseWriterException e) {
			} catch (SQLException e) {
			}
			try {
//				维持数据库连接
				this.connectionObject.getConnection().getMetaData();
			} catch (SQLException e) {
//				e.printStackTrace();
			}
		}, 10000, 10 * 1000, TimeUnit.MILLISECONDS);
	}
	
	private void updateLog(int currentDataCount) {
		if (this.appUUID != null) {
			if (eventLog.getEvent_starttime() == null) {
				eventLog.setEvent_starttime(new Timestamp(System.currentTimeMillis()));
			}
			eventLog.setApp_status(AppStatus.APP_WAITTING);
			eventLog.setWrite_total(++currentDataCount);// 读总量
			if (totalDataCount > 0) {
				float progress = ((float) currentDataCount / (float) totalDataCount) * 100;
				if (progress <= 100) {
					eventLog.setApp_progress(String.format("%.2f", progress) + "%");
				} else {
					eventLog.setApp_progress("100%");
				}
			} else {
				eventLog.setApp_progress("90%");
			}
		}
	}
	public static String getMD5(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			return new BigInteger(1, md.digest()).toString(16);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private UUID getUUID(final Map<String, Object> prop, String name) {
		final UUID targetUUID = (UUID) prop.get(name);
		if (targetUUID == null) {
			this.logger.error((Object) "NULL " + name);
			return null;
		}
		return targetUUID;
	}

	private void initializeTableMap() {
		this.tableMap = TargetTableMap.getInstance(this.targetUid.getUUIDString(), this);
	}

	private void initializeTypeMap() {
		this.tableToTypeHandlerMap = new TableToTypeHandlerMap(this.connectionObject.targetType());
	}

	private void initializeHandler() {
		this.handlerMap = new TreeMap<String, OperationHandler>(String.CASE_INSENSITIVE_ORDER);
		if (this.applyDDL) {
			this.logger.info("Initializing DDL handlers");
			final DDLHandler dropObjectHandler = new DropObjectHandler(this);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.TABLE_OBJ_TYPE + (Object) null,
					new CreateTableHandler(this));
			this.handlerMap.put(Constant.ALTER_OPERATION + Constant.TABLE_OBJ_TYPE + (Object) null,
					new AlterTableHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.TABLE_OBJ_TYPE + (Object) null, dropObjectHandler);
			this.handlerMap.put(Constant.TRUNCATE_OPERATION + Constant.TABLE_OBJ_TYPE + (Object) null,
					new TruncateTableHandler(this));
			this.handlerMap.put(Constant.RENAME_OPERATION + Constant.UKNOWN_OBJ_TYPE + (Object) null,
					new RenameTableHandler(this));
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.INDEX_OBJ_TYPE + (Object) null,
					new CreateIndexHandler(this));
			this.handlerMap.put(Constant.ALTER_OPERATION + Constant.INDEX_OBJ_TYPE + (Object) null,
					new AlterIndexHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.INDEX_OBJ_TYPE + (Object) null, dropObjectHandler);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.VIEW_OBJ_TYPE + (Object) null,
					new CreateViewHandler(this));
			this.handlerMap.put(Constant.ALTER_OPERATION + Constant.VIEW_OBJ_TYPE + (Object) null,
					new AlterViewHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.VIEW_OBJ_TYPE + (Object) null, dropObjectHandler);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.SEQUENCE_OBJ_TYPE + (Object) null,
					new CreateSequenceHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.SEQUENCE_OBJ_TYPE + (Object) null,
					dropObjectHandler);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.FUNCTION_OBJ_TYPE + (Object) null,
					new CreateFunctionHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.FUNCTION_OBJ_TYPE + (Object) null,
					dropObjectHandler);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.PROCEDURE_OBJ_TYPE + (Object) null,
					new CreateProcedureHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.PROCEDURE_OBJ_TYPE + (Object) null,
					dropObjectHandler);
			this.handlerMap.put(Constant.CREATE_OPERATION + Constant.PACKAGE_OBJ_TYPE + (Object) null,
					new CreatePackageHandler(this));
			this.handlerMap.put(Constant.DROP_OPERATION + Constant.PACKAGE_OBJ_TYPE + (Object) null, dropObjectHandler);
			final DDLHandler analyzeHandler = new AnalyzeHandler(this);
			this.handlerMap.put(Constant.ANALYZE_OPERATION + Constant.TABLE_OBJ_TYPE + (Object) null, analyzeHandler);
			this.handlerMap.put(Constant.ANALYZE_OPERATION + Constant.CLUSTER_OBJ_TYPE + (Object) null, analyzeHandler);
			final DDLHandler grantHandler = new GrantHandler(this);
			this.handlerMap.put(Constant.GRANT_OPERATION + Constant.UNKONWN_OBJECT_TYPE + (Object) null, grantHandler);
			this.handlerMap.put(Constant.REVOKE_OPERATION + Constant.UNKONWN_OBJECT_TYPE + (Object) null, grantHandler);
			this.handlerMap.put(Constant.CREATE_TABLE_AS, new CTASHandler(this));
			final DDLHandler passThroughHandler = new PassThroughDDLHandler(this);
			this.handlerMap.put(Constant.PURGE_OPERATION + Constant.UNKONWN_OBJECT_TYPE + (Object) null,
					passThroughHandler);
		}
		this.dmlHandlers = new OperationHandler[4];
		final OperationHandler insertHandler = new InsertHandler(this);
		final OperationHandler updateHandler = new UpdateHandler(this);
		final OperationHandler pkUpdateHandler = new PkUpdateHandler(this);
		final OperationHandler deleteHandler = new DeleteHandler(this);
		this.dmlHandlers[0] = insertHandler;
		this.dmlHandlers[1] = updateHandler;
		this.dmlHandlers[2] = pkUpdateHandler;
		this.dmlHandlers[3] = deleteHandler;
		this.handlerMap.put(Constant.INSERT_OPERATION + (Object) null + (Object) null, insertHandler);
		this.handlerMap.put(Constant.SELECT_OPERATION + (Object) null + (Object) null, insertHandler);
		this.handlerMap.put(Constant.UPDATE_OPERATION + (Object) null + (Object) null, updateHandler);
		this.handlerMap.put(Constant.UPDATE_OPERATION + (Object) null + "FALSE", updateHandler);
		this.handlerMap.put(Constant.UPDATE_OPERATION + (Object) null + "TRUE", pkUpdateHandler);
		this.handlerMap.put(Constant.DELETE_OPERATION + (Object) null + (Object) null, deleteHandler);
	}

	private void initializeSQLParser() {
		this.sqlParser = new TGSqlParser(EDbVendor.dbvoracle);
	}

	private void initializeCheckpointTable(final Property prop, final DatabaseWriterConnection connection)
			throws DatabaseWriterException {
		try {
			this.chkPntTblKey = this.targetUid.getUUIDString();
			(this.chkPntTblImpl = CheckpointTableImpl.getInstance(prop, connection)).initialize();
			this.chkPntTblImpl.verifyCheckpointTable();
			final Position chkPntPos = this.chkPntTblImpl.fetchPosition(this.chkPntTblKey);
			if (chkPntPos != null) {
				this.consolidatedPosition = new PathManager(chkPntPos);
			} else {
				this.consolidatedPosition = new PathManager();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 判断是否文件导入
	 * 
	 * @param event
	 * @return
	 */
	private OperationHandler getOperationFileHandler(final HDEvent event) {
		OperationHandler handler = null;
		if (event.metadata.get("FileName") == null) {
			return null;
		}
		String fileName = event.metadata.get("FileName").toString();
		if (fileName != null) {
			return new InsertHandler(this);
		}
		return handler;
	}

	private String getOperationHandlerKey(final HDEvent event) {
		final String operationName = (String) event.metadata.get(Constant.OPERATION);
		final String objectType = (String) event.metadata.get(Constant.CATALOG_OBJ_TYPE);
		final String pkUpdate = (String) event.metadata.get("PK_UPDATE");
		return operationName + objectType + pkUpdate;
	}

	private OperationHandler getOperationHandler(final HDEvent event) throws OperationNotSupported {
		OperationHandler handler = null;
		String subOpName = (String) event.metadata.get(Constant.OPERRATION_SUB_NAME);
		subOpName = ((subOpName == null) ? "" : subOpName);
		if ((handler = this.handlerMap.get(subOpName)) == null) {
			final String key = this.getOperationHandlerKey(event);
			handler = this.handlerMap.get(key);
			if (handler == null) {
				if ((handler = getOperationFileHandler(event)) == null) {
					String operation = (String) event.metadata.get(Constant.OPERATION);
					final String objType = (String) event.metadata.get(Constant.CATALOG_OBJ_TYPE);
					operation = operation + "-" + objType;
					throw new OperationNotSupported(operation);
				}
			}
		}
		return handler;
	}

	public void honurTxnBoundary(final boolean flag) {
		this.supportTxnBoundary = flag;
	}

	public TargetTableMap getTableMap() {
		return this.tableMap;
	}

	public void setAutoCommit(final boolean flag) throws DatabaseWriterException {
		try {
			this.connectionObject.getConnection().setAutoCommit(flag);
		} catch (SQLException e) {
			final DatabaseWriterException exception = new DatabaseWriterException(Error.FAILURE_IN_SQL_QUERY,
					flag + " \n ErrorCode : " + e.getErrorCode() + ";SQLCode : " + e.getSQLState() + ";SQL Message : "
							+ e.getMessage());
			this.logger.error(exception.getMessage());
			throw exception;
		}
	}

	public DatabaseWriterConnection getConnectionObject() {
		return this.connectionObject;
	}

	public long getRowCount() {
		return this.rowCount;
	}

	boolean isBoundaryTxn(final Event event) {
		if (((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("BEGIN")
				|| ((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("COMMIT")) {
			return true;
		}
		final String booleanString = (String) ((HDEvent) event).metadata.get("Rollback");
		return "1".equals(booleanString);
	}

	public void processEvents(final int channel, final Event event, final Position position)
			throws DatabaseWriterException, SQLException {
		if (this.lastException != null) {
			throw new DatabaseWriterException(Error.QUERY_PROCESSING_FAILURE, "", this.lastException);
		}
		try {
			if (this.supportTxnBoundary && this.isBoundaryTxn(event)) {
				this.processTxnBounary(event);
				return;
			}
			final HDEvent HDEvent = (HDEvent) event;
//			System.out.println("-----"+HDEvent.toJSON());
			boolean isDDL = false;
			if (HDEvent.metadata.get("IsDDL") != null) {
				isDDL = (boolean) HDEvent.metadata.get("IsDDL");
			}
			if(HDEvent.metadata.containsKey("OperationType") && "DDL".equalsIgnoreCase((String)HDEvent.metadata.get("OperationType"))) {
				isDDL = true;
			}
			if (isDDL) {
				
				String sql = HDEvent.metadata.get("SQL") == null ? (HDEvent.data != null && HDEvent.data.length > 0 ? (String)HDEvent.data[0]:null) : (String) HDEvent.metadata.get("SQL");
				if (sql != null) {
					if (this.tables.length == 2) {
						List<String> list = null;
						EDbVendor sourceDbType = null;
						EDbVendor targetDbType = null;
						String sourceType = HDEvent.metadata.get("SourceType") == null ? null
								: HDEvent.metadata.get("SourceType").toString();
						String targetType = this.connectionObject.targetType();
						switch (sourceType.toLowerCase()) {
						case "postgresql":
							sourceDbType = EDbVendor.dbvmysql;
							break;
						case "mysql":
							sourceDbType = EDbVendor.dbvmysql;
							break;
						case "oracle":
							sourceDbType = EDbVendor.dbvoracle;
							break;
						default:
							break;
						}

						switch (targetType.toLowerCase()) {
						case "postgresql":
							targetDbType = EDbVendor.dbvpostgresql;
							break;
						case "mysql":
							targetDbType = EDbVendor.dbvmysql;
							break;
						case "oracle":
							targetDbType = EDbVendor.dbvoracle;
							break;
						default:
							break;
						}
						if(HDEvent.metadata.containsKey("OperationSubName")) {
							switch ((String)HDEvent.metadata.get("OperationSubName")) {
							case "ALTER_TABLE_ADD_COLUMN":{
								
								break;
							}
							case "ALTER_TABLE_DROP_COLUMN":
							case "ALTER_TABLE_MODIFY_COLUMN":
							case "ALTER_TABLE_RENAME_COLUMN":
							case "CREATE_TABLE":
							case "ALTER_TABLE_RENAME":
							case "ALTER_TABLE_DROP_CONSTRAINT_PK":
							case "CTAS": {
//								this.updateMetadataRecordCacheAndUUIDCache(ddlRecord, out);
								break;
							}
							case "ALTER_TABLE_DROP_CONSTRAINT": {
//								final String constrainType = ddlRecord.getParameterValue("ConstraintType");
//								if (constrainType.contentEquals("Primary") || constrainType.contentEquals("Unique")) {
//									this.updateMetadataRecordCacheAndUUIDCache(ddlRecord, out);
//								}
								break;
							}
							case "ALTER_TABLE_ADD_CONSTRAINT": {
//								final String constrainType = ddlRecord.getParameterValue("ConstraintType");
//								if (constrainType.contentEquals("Primary")) {
//									this.updateMetadataRecordCacheAndUUIDCache(ddlRecord, out);
//								}
								break;
							}
							}
						}
						if (sourceDbType != null && targetDbType != null) {
							list = new SqlParser(sourceDbType, targetDbType, this.tables[1]).ddlParse(sql);
							Statement stat = null;
							try {
								stat = this.connectionObject.getConnection().createStatement();
								for (String str : list) {
									System.out.println("----转换为目标 DDL SQL:" + str);
									stat.execute(str);
								}
							} catch (SQLException e) {
								e.printStackTrace();
								throw new DatabaseWriterException(Error.FAILURE_IN_BEGIN,
										" \n ErrorCode : " + e.getErrorCode() + ";SQLCode : " + e.getSQLState()
												+ ";SQL Message : " + e.getMessage());
							} finally {
								try {
									if (stat != null) {
										stat.close();
									}
								} catch (Exception e) {

								}
							}
						}
					}
				}
				return;
			}
			final OperationHandler opHandler = this.getOperationHandler(HDEvent);
			opHandler.validate(HDEvent);
			final String sourceName = opHandler.getComponentName(HDEvent);
			if (opHandler.objectBasedOperation()) {
				if (sourceName != null) {
					final ArrayList<String> listOfTargets = this.wildcardProcessor.getMapForSourceTable(sourceName);
					if (listOfTargets == null) {
						// if (this.logger.isInfoEnabled()) {
						this.logger.info("Ignoring HDEvent as table as " + sourceName
								+ " is not in the list of tables to be captured.");
						// }
						return;
					}
					for (final String tableName : listOfTargets) {
						if (opHandler.isDDLOperation() || this.tableMap.getTableMeta(tableName) != null) {
							try {
								this.policy.add(HDEvent, position, tableName);
							}catch(Exception e) {
								this.policy.add(HDEvent, position, tableName.toUpperCase());
							}
						} else {
							if (!this.logger.isInfoEnabled()) {
								continue;
							}
							this.logger.info("Since target table {" + tableName
									+ "} is not found, not processing the event of {" + sourceName + "} for it");
						}
					}
				}
			} else {
				this.policy.add(HDEvent, position, sourceName);
			}
		} catch (OperationNotSupported opExp) {
			this.logger.warn(opExp.getMessage());
		}
	}

	public void processTxnBounary(final Event event) throws DatabaseWriterException {
		String operationStr = "";
		final Object operation = ((HDEvent) event).metadata.get("OperationName");
		if (operation != null) {
			try {
				synchronized (this.connectionObject) {
					if (this.connectionObject.getConnection().isClosed()) {
						this.logger.warn("DatabaseConnection is closed, not proceeding furhter");
					}
					operationStr = ((String) operation).toLowerCase();
					if (operationStr.equals("commit")) {
						this.commit();
						this.savePoint = null;
					} else if (operationStr.equals("begin")) {
						this.savePoint = this.connectionObject.getConnection().setSavepoint("HD_SAVE_PONT");
					} else if ("1".equals(((HDEvent) event).metadata.get("Rollback"))) {
						if (this.savePoint != null) {
							this.connectionObject.getConnection().rollback(this.savePoint);
							this.savePoint = null;
						} else {
							this.logger.error("Got NULL SavePoint while rolling back transactions");
						}
					} else {
						this.logger.error("Unsupported transaction boundary transaction received");
					}
				}
			} catch (SQLException exp) {
				DatabaseWriterException exception = null;
				if (operationStr.equals("commit")) {
					exception = new DatabaseWriterException(Error.FAILURE_IN_COMMIT, " \n ErrorCode : "
							+ exp.getErrorCode() + ";SQLCode : " + exp.getSQLState() + ";SQL Message : ");
				} else if (operationStr.equals("begin")) {
					exception = new DatabaseWriterException(Error.FAILURE_IN_BEGIN, " \n ErrorCode : "
							+ exp.getErrorCode() + ";SQLCode : " + exp.getSQLState() + ";SQL Message : ");
				} else {
					exception = new DatabaseWriterException(Error.FAILURE_IN_ROLLBACK, " \n ErrorCode : "
							+ exp.getErrorCode() + ";SQLCode : " + exp.getSQLState() + ";SQL Message : ");
				}
				throw exception;
			}
		}
	}

	public void endProcessing() throws DatabaseWriterException, SQLException {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Closing DatabaseWriter");
		}
		this.policy.close();
		this.tableMap.clear();
		if(pool!=null) {
			pool.purge();
			pool.shutdown();
		}
	}
	private boolean stopApp(HDEvent evt) {
		try {
			System.out.println("任务 " + this.appName + " 同步完成，累计同步数据量：" + this.rowCount + "条，同步结束停止任务。");
			logger.info("任务 " + this.appName + " 同步完成，累计同步数据量：" + this.rowCount + "条，同步结束停止任务。");
			commitCount = 0;
			commit();
			com.datasphere.runtime.meta.MetaInfo.MetaObject metaObject = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, HDSecurityManager.TOKEN);
//			System.out.println("--==" + metaObject.toString());
			if (metaObject instanceof com.datasphere.runtime.meta.MetaInfo.Target) {
//				System.out.println("----停止应用");
				com.datasphere.runtime.meta.MetaInfo.Target current_stream = (com.datasphere.runtime.meta.MetaInfo.Target) metaObject;
				AppManager appmgr = new AppManager(BaseServer.getBaseServer());
				try {
					appmgr.changeApplicationState(ActionType.PAUSE_SOURCES, current_stream.getCurrentApp().getUuid(),null, null);
				} catch (Exception e) {

				}
				appmgr.changeApplicationState(ActionType.STOP, current_stream.getCurrentApp().getUuid(), null, null);
				
			}
			return true;
		} catch (Exception e) {
			
		}
		return false;
	}
	
	/**
	 * 停止本次同步日志
	 */
	private void stopLog() {
		try {
			updating = false;
			if (this.appUUID != null) {
				eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
				eventLog.setEvent_total((int)this.batchedEventCount);// 数据交换总量
				eventLog.setApp_status(AppStatus.APP_FINISHED);
				eventLog.setApp_progress("100%");
				exceptionLogUtils.insertOrUpdate(eventLog); /** 记录数据Event的日志 和 数据交换日志 */
				this.connectionObject.getConnection().commit();
			}
		}catch(Exception e) {
			
		}
	}
	@Override
	public int execute(final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
		try {
			executeimpl(events);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseWriterException(e.getMessage());
		}
		return 0;
	}

	private int executeimpl(final LinkedList<EventData> events)
			throws SQLException, DatabaseWriterException, AdapterException {
		commitCount = 0;
		synchronized (this.connectionObject) {
			if (events.isEmpty()) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Got empty batch, skipping it");
				}
				stopLog();
				return 0;
			}
			
//			System.out.println("----接收到消息1："+events.get(0).toString());
			String targetType = this.connectionObject.targetType();
			final HDEvent event = (HDEvent) events.get(0).event;
			event.metadata.put("TargetType", targetType);
			//判断元数据信息中是否包含已完成状态，如包含，则停止任务
			if((event != null && (event.metadata.containsKey("status") && "Completed".equalsIgnoreCase(event.metadata.get("status").toString())))) {
				stopApp(event);
				return 0;
			}
			// System.out.println("----接收到消息："+event.toJSON());
			// final String tblName = (String) event.metadata.get("TableName");
			boolean isDDL = event.metadata.get("IsDDL") == null ? false : (boolean) event.metadata.get("IsDDL");
			String sourceType = event.metadata.get("SourceType") == null ? ""
					: (String) event.metadata.get("SourceType");
			OperationHandler handler = null;
			try {
				handler = this.getOperationHandler(event);
			} catch (OperationNotSupported opExp) {
				throw new DatabaseWriterException(opExp.toString());
			}
			final String target = events.get(0).target;
			final Table tbl = (Table) this.tableMap.getTableMeta(target);
			if (tbl != null) {
				String query = "";
				try {
					if (isDDL && Constant.MYSQL_TYPE.equalsIgnoreCase(sourceType)) {

						String sql = event.metadata.get("SQL") != null ? null : (String) event.metadata.get("SQL");
						if (sql != null) {
							List<String> list = new SqlParser(EDbVendor.dbvmysql, EDbVendor.dbvpostgresql,
									tbl.getName()).ddlParse(sql);
							Statement stat = null;
							try {
								stat = this.connectionObject.getConnection().createStatement();
								for (String str : list) {
									stat.execute(str);
								}
							} catch (Exception e) {
								e.printStackTrace();
							} finally {
								try {
									if (stat != null)
										stat.close();
								} catch (Exception e) {

								}
							}
						}
						return 0;
					}

					try {
						query = handler.generateDML(event, target);
					} catch (NullPointerException e) {
						e.printStackTrace();
						Target current_stream = (Target) MetadataRepository.getINSTANCE()
								.getMetaObjectByUUID(this.targetUid, HDSecurityManager.TOKEN);
						AppManager appmgr = new AppManager(BaseServer.getBaseServer());
						appmgr.changeApplicationState(ActionType.STOP, current_stream.getCurrentApp().getUuid(), null,
								null);
						logger.info("----网络异常，正在停止应用，请稍后重启应用");
						return 0;
					}
				} catch (DatabaseWriterException exp) {
					exp.printStackTrace();
					if (exp.getErrorCode() == Error.NO_OP_UPDATE.getType()) {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Ignoring NO-OP operation");
						}
						return 0;
					}
					throw exp;
				} catch (Exception e) {
					e.printStackTrace();
					return 0;
				}
				int rowsAffected = 0;
				PreparedStatement stmt = this.preparedStmtCache.get(query);
				if (stmt == null) {
					stmt = this.connectionObject.getConnection().prepareStatement(query);
					this.preparedStmtCache.put(query, stmt);
				}
				if (events.size() > 0) {
					try {
						isCommited = false;
						// System.out.println("----执行插入："+query);
						// System.out.println("----数据："+event.toJSON());
						rowsAffected = this.executionPolicy.execute(handler, tbl.getFullyQualifiedName(), query, stmt,
								events);
						isCommited = true;
						commitCount = 0;
					} catch (Exception e) {
//						System.out.println("---" + e.getMessage());
						if (e instanceof java.sql.BatchUpdateException || e.getMessage().indexOf("'PRIMARY'") > 0
								|| e.getMessage().indexOf("ORA-00001") > 0) {
							for (EventData data : events) {
								try {
									LinkedList<EventData> list = new LinkedList<>();
									list.add(data);
									isCommited = false;
									rowsAffected = this.executionPolicy.execute(handler, tbl.getFullyQualifiedName(),
											query, stmt, list);
									isCommited = true;
									commitCount = 0;
									list = null;
								} catch (Exception e2) {
									if (e2.getMessage().indexOf("'PRIMARY'") < 0
											&& e2.getMessage().indexOf("ORA-00001") < 0) {
										logger.error("单条执行语句出错：" + e2.getMessage());
										throw new SQLException(e2);
									} else {
										if (logger.isDebugEnabled()) {
											logger.debug("单条提交主键已存在，忽略");
										}
									}
								}
							}
						} else {
							System.out.println("执行语句出错：" + e.getMessage());
							throw new AdapterException("执行语句出错：" + e.getMessage());

						}
					}
					this.rowCount += rowsAffected;
					this.batchedEventCount = rowsAffected;
					this.tmpCommitEventCount += rowsAffected;

					updateLog(rowsAffected);
					return rowsAffected;
				}
			}
		}
		return 0;
	}

	/**
	 * 得到源表和目标表表名
	 * 
	 * @param properties
	 * @return
	 */
	private String[] getTables(final Map<String, Object> properties) {
		Property prop = new Property(properties);
		String tables = prop.getString("Tables", "");
		if (tables != null) {
			String[] tb = tables.split(";");
			if (tb.length > 0) {
				return tb[0].split(",");
			}
		}
		return null;
	}

	@Override
	public void commit() throws SQLException, DatabaseWriterException {
		isCommited = false;
		synchronized (this.connectionObject) {
			if (this.connectionObject.getConnection() != null && !this.connectionObject.getConnection().isClosed()) {
				try {
					this.chkPntTblImpl.updateCheckPointTable(this.consolidatedPosition.toPosition(), null);
				} catch (Exception e) {

				}
				if (this.tmpCommitEventCount != 0L) {
					this.eventsInLastCommit = this.tmpCommitEventCount;
					this.tmpCommitEventCount = 0L;
				}
				this.lastCommitTime = DateTime.now();
				if (!this.connectionObject.getConnection().isClosed())
					this.connectionObject.getConnection().commit();
				if (this.consolidatedPosition != null) {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Acknowledging {" + this.consolidatedPosition + "}");
					}
					this.setChanged();
					this.notifyObservers(this.consolidatedPosition);
				}
			}
		}
		isCommited = true;
	}

	@Override
	public boolean onError(final Event event, final Exception exp) {
		if (exp.getMessage().indexOf("'PRIMARY'") > 0) {
			return true;
		}
		Exception nextExp = null;
		String errMsg = "Got exception : ";
		if (exp instanceof SQLException) {
			nextExp = ((SQLException) exp).getNextException();
			if (nextExp != null) {
				errMsg = errMsg + "{" + nextExp.getMessage() + "}";
			}
		}
		this.setChanged();
		final ExceptionData expData = new ExceptionData(exp, (HDEvent) event);
		this.notifyObservers(expData);
		if (!expData.ignored) {
			try {
				this.policy.close();
			} catch (DatabaseWriterException | SQLException ex) {
			}
		} else {
			this.policy.reset();
		}
		return expData.ignored;
	}

	@Override
	public void reset() {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Resetting Policy");
		}
		this.policy.reset();
	}

	@Override
	public void updateConsolidatedPosition(final Position eventPosition) {
		if (this.consolidatedPosition != null) {
			this.consolidatedPosition.mergeHigherPositions(eventPosition);
		} else {
			this.consolidatedPosition = new PathManager(eventPosition);
		}
	}

	@Override
	public void close() throws SQLException {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Closing Database connection");
		}
		
		synchronized (this.connectionObject) {
			for (final Map.Entry<String, PreparedStatement> entry : this.preparedStmtCache.entrySet()) {
				entry.getValue().close();
			}
			this.connectionObject.getConnection().close();
		}
		String fileName = System.getProperty("user.dir") + File.separator + this.appUUID.toString() + ".csv";
		try {
			File file = new File(fileName);
			if (file.exists()) {
				file.delete();
			}
			
		} catch (Exception e) {

		}
		try {
			if (this.pool != null) {
				this.pool.shutdown();
				this.pool.purge();
			}
		}catch(Exception e) {
			
		}
	}

	@Override
	public DatabaseTable getTableMeta(final String tableName) throws DatabaseWriterException {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Loading table metadata for {" + tableName + "}");
		}
		final DatabaseWriterConnection.TableNameParts tps = this.connectionObject.getTableNameParts(tableName);
		try {
			if (tps.catalog == null) {
				tps.catalog = this.currentCatalog;
			}
			if (tps.schema == null) {
				tps.schema = this.currentSchema;
			}
			if (tps.catalog == null) {
				tps.catalog = this.getConnectionObject().getConnection().getCatalog();
			}
			if (tps.schema == null) {
				tps.schema = this.getConnectionObject().getConnection().getSchema();
			}

//            System.out.println("---db.catalog:"+tps.catalog);
//            System.out.println("---db.schema:"+tps.schema);
//            System.out.println("---db.table:"+tps.table);
            
			final DatabaseMetaData dbMeta = this.getConnectionObject().getConnection().getMetaData();
			ResultSet tableInfoResultSet = dbMeta.getTables(tps.catalog, tps.schema, tps.table,
					DatabaseWriterProcessEvent.entityTypes);
			
			
			ResultSet columnInfoResultSet = dbMeta.getColumns(tps.catalog, tps.schema, tps.table, null);
			
			ResultSet tableKeyResultSet = dbMeta.getPrimaryKeys(tps.catalog, tps.schema, tps.table);
			
			
			if(tps.table.equalsIgnoreCase("CHKPOINT") && !"oracle".equalsIgnoreCase(this.connectionObject.targetType())) {
				if (tableInfoResultSet == null || !tableInfoResultSet.next()) {
					tableInfoResultSet = dbMeta.getTables(tps.catalog, tps.schema, tps.table.toUpperCase(),
							DatabaseWriterProcessEvent.entityTypes);
				}
				if (columnInfoResultSet == null || !columnInfoResultSet.next()) {
					columnInfoResultSet = dbMeta.getColumns(tps.catalog, tps.schema, tps.table.toUpperCase(), null);
				}
				if (tableKeyResultSet == null || !tableKeyResultSet.next()) {
					tableKeyResultSet = dbMeta.getPrimaryKeys(tps.catalog, tps.schema, tps.table.toUpperCase());
				}
				try {
					tableInfoResultSet.beforeFirst();
				}catch(Exception e) {
					
				}
				try {
					columnInfoResultSet.beforeFirst();
				}catch(Exception e) {
					
				}
				try {
					tableKeyResultSet.beforeFirst();
				}catch(Exception e) {
					
				}
			}
			
			
			
//			ResultSet tableInfoResultSet = dbMeta.getTables(tps.catalog, tps.schema, tps.table,
//					DatabaseWriterProcessEvent.entityTypes);
//			ResultSet columnInfoResultSet = dbMeta.getColumns(tps.catalog, tps.schema, tps.table, null);
//			ResultSet tableKeyResultSet = dbMeta.getPrimaryKeys(tps.catalog, tps.schema, tps.table);
			Table dbTable = new Table(tableInfoResultSet, columnInfoResultSet, tableKeyResultSet);
			tableInfoResultSet.close();
			columnInfoResultSet.close();
			tableKeyResultSet.close();
			if (!dbTable.isValid()) {
				throw new DatabaseWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, "表 {" + tableName + "} 不存在");
			}
			return dbTable;
		} catch (SQLException sExp) {
			final String fullyQualifiedName = this.connectionObject.fullyQualifiedName(tableName);
			throw new DatabaseWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS,
					"获取表 {" + fullyQualifiedName + "} 元数据时出现异常", sExp);
		}
	}

	@Override
	public void onDDL(final EventData ddl) throws SQLException, DatabaseWriterException {
		final HDEvent HDEvent = (HDEvent) ddl.event;
		OperationHandler handler = null;
		synchronized (this.connectionObject) {
			if (this.connectionObject.getConnection().isClosed()) {
				return;
			}
			try {
				handler = this.getOperationHandler(HDEvent);
			} catch (OperationNotSupported opExp) {
				throw new DatabaseWriterException(opExp.toString());
			}
			final String ddlCmd = handler.generateDDL((HDEvent) ddl.event, ddl.target);
			if (this.possiblePendingDDL) {
				this.possiblePendingDDL = this.chkPntTblImpl.pendingDDL();
				if (this.possiblePendingDDL) {
					if (!ddlCmd.equals(this.chkPntTblImpl.ddl())) {
						this.logger.warn("Got different DDL, we suppose to execute {" + this.chkPntTblImpl.ddl()
								+ "} but we got {" + ddlCmd + "}");
					}
					if (this.logger.isDebugEnabled()) {
						final String lastDDL = this.chkPntTblImpl.ddl();
						this.logger.debug(
								"Last DDL operation {" + lastDDL + "} may not be complete, going to reapply the same");
					}
				}
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Executing DDL {" + ddlCmd + "}");
			}
			try {
				final PreparedStatement ddlStmt = this.connectionObject.getConnection().prepareStatement(ddlCmd);
				if (ddl.position != null) {
					this.chkPntTblImpl.updateCheckPointTable(this.consolidatedPosition.toPosition(), ddlCmd);
				}
				ddlStmt.execute();
			} catch (SQLException exp) {
				if (!this.possiblePendingDDL) {
					throw exp;
				}
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Suppressing sql exception {" + exp.getMessage() + "}");
				}
			}
			this.possiblePendingDDL = false;
			if (ddl.position != null) {
				this.updateConsolidatedPosition(ddl.position);
				this.chkPntTblImpl.updateCheckPointTable(this.consolidatedPosition.toPosition(), null);
			}
		}
		handler.onDDLOperation(ddl.target, HDEvent);
	}

	private void invalidateSQLStatementCache(final String table) {
		for (int itr = 0; itr < this.dmlHandlers.length; ++itr) {
			this.dmlHandlers[itr].invalidateCacheFor(table);
		}
	}

	@Override
	public void onCreateTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
		final String columnDetails = (String) event.metadata.get(Constant.TABLE_META_DATA);
		try {
			final JSONObject columnMetadata = new JSONObject(columnDetails);
			this.tableMap.addNewTable(targetTable, columnMetadata);
		} catch (JSONException e) {
			throw new DatabaseWriterException("JSON解析问题 {" + e.getMessage() + "}");
		}
	}

	@Override
	public void onAlterTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
		final String columnDetails = (String) event.metadata.get(Constant.TABLE_META_DATA);
		if (columnDetails != null) {
			try {
				final JSONObject columnMetadata = new JSONObject(columnDetails);
				this.invalidateSQLStatementCache(targetTable);
				this.tableMap.updateTableDetails(targetTable, columnMetadata);
				this.tableToTypeHandlerMap.invalidateMappingFor(targetTable);
				return;
			} catch (JSONException e) {
				throw new DatabaseWriterException("JSON解析问题 {" + e.getMessage() + "}");
			}
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Alter command did not have {" + Constant.TABLE_META_DATA + "}, not updating internal cache");
		}
	}

	@Override
	public void onDropTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
		this.invalidateSQLStatementCache(targetTable);
		this.tableToTypeHandlerMap.invalidateMappingFor(targetTable);
	}

	@Override
	public void onRenameTable(final String targetTable, final HDEvent event) throws DatabaseWriterException {
		final String newObjectName = (String) event.metadata.get("NewObjectName");
		final String oldObjectName = (String) event.metadata.get("OldObjectName");
		final String columnDetails = (String) event.metadata.get(Constant.TABLE_META_DATA);
		if (columnDetails != null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Renaming Object {" + oldObjectName + "} as {" + newObjectName + "}");
			}
			try {
				final JSONObject columnMetadata = new JSONObject(columnDetails);
				this.tableToTypeHandlerMap.invalidateMappingFor(oldObjectName);
				this.invalidateSQLStatementCache(oldObjectName);
				this.tableMap.removeTableDetails(oldObjectName);
				this.tableMap.addNewTable(newObjectName, columnMetadata);
				return;
			} catch (JSONException e) {
				throw new DatabaseWriterException(
						"解析RENAME操作 " + Constant.TABLE_META_DATA + " 时出错 {" + e.getMessage() + "}");
			}
		}
		throw new DatabaseWriterException(
				Constant.TABLE_META_DATA + " is required for RENAME which is  missing in HDEvent");
	}

	public Position getWaitPositionFromWriter(final String key) throws DatabaseWriterException {
		return this.chkPntTblImpl.fetchPosition(key);
	}

	@Override
	public TGSqlParser getSQLParser() {
		return this.sqlParser;
	}

	@Override
	public WildCardProcessor getWildcardProcessor() {
		return this.wildcardProcessor;
	}

	@Override
	public DatabaseWriterConnection.TableNameParts getTableNameParts(final String tableName) {
		return this.connectionObject.getTableNameParts(tableName);
	}

	@Override
	public String getSchema() throws SQLException {
		return this.currentSchema;
	}

	public DateTime lastCommitTime() {
		return this.lastCommitTime;
	}

	public long eventCount() {
		return this.rowCount;
	}

	public long executionLatency() {
		return this.executionLatency;
	}

	public long batchedEventCount() {
		return this.batchedEventCount;
	}

	public DateTime lastBatchExecuteTime() {
		return this.lastBatchExecuteTime;
	}

	public long eventsInLastCommit() {
		return this.eventsInLastCommit;
	}

	@Override
	public SourceTableMap getSourceTableMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TargetTableMap getTargetTableMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Property getProperty() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onDrop() throws DatabaseWriterException {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {
		// String content = "{fasdf,2019-06-02T12:12:12.000+08:00}";
		// content = content.substring(1, content.length()-1);
		// System.out.println(content);
		// appendFile("abc.csv", content, true);
		// TGSqlParser parser = new TGSqlParser(EDbVendor.dbvmysql);
		// parser.sqltext = sql;
		// parser.parse();
		//
		// if (parser.getErrorCount() == 0)
		// {
		// TStatementList stmts = parser.sqlstatements;
		// for (int i = 0; i< stmts.size(); i++) {
		// TCustomSqlStatement stmt = stmts.get(i);
		// if(stmt.sqlstatementtype == ESqlStatementType.sstcreatetable ||
		// stmt.sqlstatementtype == ESqlStatementType.sstcreateview) {
		//
		// }else if(stmt.sqlstatementtype == ESqlStatementType.sstcreateindex) {
		//
		// }else if(stmt.sqlstatementtype == ESqlStatementType.sstdropindex) {
		//
		// }else if(stmt.sqlstatementtype == ESqlStatementType.sstaltertable) {
		//
		// }else if(stmt.sqlstatementtype == ESqlStatementType.sstTruncate) {
		//
		// }
		// TTableList list = stmt.tables;
		// for(int j = 0; j< list.size(); j++) {
		// TTable table = list.getTable(j);
		//
		// System.out.println(table.getFullName());
		// }
		// }
		// }
		// System.out.println(sql.trim());
	}

	@Override
	public Connection getConnection() {
		return this.connectionObject.getConnection();
	}

	@Override
	public TableToTypeHandlerMap getTableToTypeHandlerMap() {
		return this.tableToTypeHandlerMap;
	}

	static {
		DatabaseWriterProcessEvent.entityTypes = new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" };
	}

	public class EventDetails {
		public Event event;
		public Position position;
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
}
