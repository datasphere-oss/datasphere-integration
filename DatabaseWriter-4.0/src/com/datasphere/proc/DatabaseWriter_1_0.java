package com.datasphere.proc;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.InvalidDataException;
import com.datasphere.common.exc.SystemException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.Acknowledgeable;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.MetaInfo.Target;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.runtime.utils.MapFactory;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.Utils;
import com.datasphere.uuid.UUID;
import com.datasphere.Policy.PolicyFactory;
import com.datasphere.databasewriter.DatabaseWriter;
import com.datasphere.databasewriter.ExceptionData;
import com.datasphere.proc.common.AppStatus;
import com.datasphere.proc.entity.EventLog;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "DatabaseWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Username", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "ExcludedTables", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "PreserveSourceTransactionBoundary", type = Boolean.class, required = false, defaultValue = "false"),
		@PropertyTemplateProperty(name = "BatchPolicy", type = String.class, required = false, defaultValue = "EventCount:15000,Interval:2"),
		@PropertyTemplateProperty(name = "IgnorableExceptionCode", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "CheckPointTable", type = String.class, required = false, defaultValue = "CHKPOINT"),
		@PropertyTemplateProperty(name = "CommitPolicy", label = "提交策略", type = String.class, required = false, defaultValue = "EventCount:15000,Interval:2") }, outputType = HDEvent.class)
public class DatabaseWriter_1_0 extends BaseProcess implements Acknowledgeable, Observer {
	private static Logger logger;
	private DatabaseWriter writer;
	protected boolean acknowledgePosition;
	Map<String, String> ignorableExceptionCode;
	Exception exception;
	String checkPointTableKey;
	long ignoredExceptionCount;
	PathManagerExtn waitPosition;
	DateTime lastCommit;
	boolean closeCalled;
	boolean isPostgres;
	Position lastAckedPosition;
	
	private UUID appUUID;
	private UUID targetUUID;
    private String appName;
	private boolean updating;
    private ScheduledThreadPoolExecutor pool;
//    private ExceptionLogUtils exceptionLogUtils;
    private EventLog eventLog;
    private int dataCount;
    private int totalData;
    private int totalExceptionData;
    private Event currentEvent;
    private Event lastEvent;
    private int flag = 0;

	public DatabaseWriter_1_0() {
		this.writer = null;
		this.acknowledgePosition = false;
		this.closeCalled = false;
		this.updating = false;
		this.lastEvent = null;
//		this.exceptionLogUtils = new ExceptionLogUtils();
	}

	public void init(final Map<String, Object> properties, final Map<String, Object> formatterProperties,
			final UUID inputStream, final String distributionID) throws Exception {
		try {
			super.init(properties, formatterProperties, inputStream, distributionID);

//			this.exceptionLogUtils = new ExceptionLogUtils();
			this.targetUUID = (UUID) properties.get("TargetUUID");
			try {
				Target current_stream = (Target) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUUID, HDSecurityManager.TOKEN);
				this.appUUID = current_stream.getCurrentApp().getUuid();
				this.appName = current_stream.getCurrentApp().getName();
//				System.out.println("----"+this.appUUID + "," + this.appName);
			} catch (MetaDataRepositoryException e) {
//				if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "MetaDataRepositoryException", e.getMessage(),eventLog);
				e.printStackTrace();
			}
			this.waitPosition = new PathManagerExtn(null);
			this.ignorableExceptionCode = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
			final Map<String, Object> tmpMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
			tmpMap.putAll(properties);
			final String exp = (String) tmpMap.get("IgnorableExceptionCode");
			if (exp != null) {
				final String[] expData = exp.split(",");
				for (int itr = 0; itr < expData.length; ++itr) {
					if (expData[itr].trim().length() > 0) {
						this.ignorableExceptionCode.put(expData[itr].trim(), expData[itr].trim());
					}
				}
			}
			
			(this.writer = new DatabaseWriterNew()).initDatabaseWriter(properties, formatterProperties, inputStream,
					distributionID);
			this.writer.registerObserver(this);
			this.checkPointTableKey = ((inputStream == null) ? null : inputStream.toString());
			final Property prop = new Property(tmpMap);
			final String dbURL = prop.getString(Constant.CONNECTION_URL, (String) null);
			final String dbType = Utils.getDBType(dbURL);
			this.isPostgres = (Constant.POSTGRESS_TYPE.equalsIgnoreCase(dbType)
					|| Constant.EDB_TYPE.equalsIgnoreCase(dbType));
			this.closeCalled = false;
			
			/** 记录log */
//			this.pool = new ScheduledThreadPoolExecutor(1);
//			this.eventLog = new EventLog();
//			this.eventLog.setApp_uuid(this.appUUID.getUUIDString());
//			this.eventLog.setApp_name(this.appName);
//			this.eventLog.setCluster_name(HazelcastSingleton.getClusterName());
//			this.eventLog.setNode_id(HazelcastSingleton.getBindingInterface());
//			this.eventLog.setApp_progress("0%");
//			pool.scheduleAtFixedRate(() -> {
//				if (currentEvent == null && lastEvent != null) {//更新本批数据同步最终状态
//	        			if(flag >= 1) {
//	        				this.eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()-5000));
//	    				} else {
//	    					this.eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
//	    				}
//	        			this.eventLog.setWrite_total(totalData);// 数据写入总量
//	    				this.eventLog.setEvent_total(totalData);// 数据交换总量
//	    				this.eventLog.setExce_total(totalExceptionData);// 数据异常总量
//	    				this.eventLog.setApp_status(AppStatus.APP_FINISHED);//更新本批数据同步最终状态
//	    				this.eventLog.setApp_progress("100%");
//	    				if (updating && this.eventLog.getEvent_starttime() != null) {
//	    					updating = false;
//	    					exceptionLogUtils.insertOrUpdate(this.eventLog);
//	    					updating = true;
//	    				}
//	    				revertValue();//恢复初始化状态
//				} else {//更新本批数据同步实时状态
//					if (lastEvent != null && updating && this.eventLog.getEvent_starttime() != null) {
//	//					System.out.println("178");
//						this.eventLog.setApp_status(AppStatus.APP_WRITERING);
//				        this.eventLog.setWrite_total(totalData);// 数据写入总量
//				        this.eventLog.setExce_total(totalExceptionData);// 数据异常总量
//				        if (dataCount != -1 && dataCount > 0) {
//		            		    float progress = ((float)totalData / (float)dataCount) * 100;
//		            		    if(progress < 100) {
//		            		    		this.eventLog.setApp_progress(String.format("%.2f", progress) + "%");
//		            		    } else {
//		            		    		this.eventLog.setApp_progress("100%");
//		            		    }
//	            		    } else {
//	            		    		this.eventLog.setApp_progress("100%");
//	            		    }
//						exceptionLogUtils.insertOrUpdate(this.eventLog); 
//					}
//				}
//	        		++flag;
//			}, 1000, 5 * 1000, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
//			if(exceptionLogUtils != null)  this.exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "SystemException", "Error in initialising DatabaseWriter {" + e.getMessage() + "}");
			e.printStackTrace();
			throw new SystemException("Error in initialising DatabaseWriter {" + e.getMessage() + "}", (Throwable) e);
		}
	}

	public void receive(final int channel, final Event event, final Position position) throws Exception {
		receiveImpl(channel,event);
	}
	
	public void receiveImpl(final int channel, final Event event) throws Exception {
//		noteLog(event);
//		System.out.println("----receiveImpl接收到数据："+event.toString());
		if (this.exception != null) {
			final Exception tmpExp = this.exception;
			this.exception = null;
//			if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "SystemException", "4:Error in processing event for DatabaseWriter{" + tmpExp.getMessage() + "}",eventLog);
			throw new SystemException("4:Error in processing event for DatabaseWriter", (Throwable) tmpExp);
		}
		if (event == null) {
			setValue();
			return;
		} else {
			if(event instanceof HDEvent) {
				HDEvent event2 = (HDEvent)event;
				if (event2.data == null) {
					setValue();
			        return;
				} 
			}else {
				if (event.getPayload() == null) {
					setValue();
			        return;
				}
			}
			
		}
		try {
			this.writer.processEvent(channel, event, null);
			if (this.exception != null) {
				final Exception tmpExp = this.exception;
				this.exception = null;
//				if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "SystemException", "5:Error in processing event for DatabaseWriter {" + tmpExp.getMessage() + "}",eventLog);
				throw new SystemException("5:Error in processing event for DatabaseWriter", (Throwable) tmpExp);
			}
			this.lastEvent = event;
		} catch (Exception e) {
			e.printStackTrace();
			if (e instanceof InvalidDataException) {
//				if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "InvalidDataException", e.getMessage(),eventLog);
				throw e;
			}
			if (!this.closeCalled) {
//				if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "SystemException", "6:Error in processing event for DatabaseWriter  {" + e.getMessage() + "}",eventLog);
				throw new SystemException("6:Error in processing event for DatabaseWriter {" + e.getMessage() + "}", (Throwable) e);
			}
		}
	}
	
	/**
	 * 实时更新日志数据交换记录
	 */
	private void setValue() {
		this.lastEvent = null;
		if(eventLog!= null) {
		this.eventLog.setApp_status(AppStatus.APP_FINISHED);
        this.eventLog.setWrite_total(totalData);// 数据写入总量
        this.eventLog.setExce_total(totalExceptionData);// 数据异常总量
        this.eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
		}
	}
	
	/**
	 * 恢复初始化状态
	 */
	private void revertValue() {
		lastEvent = null;
		totalData = 0;
		updating = false;
		if(eventLog!= null) {
		this.eventLog.setEvent_starttime(null);
		this.eventLog.setEvent_endtime(null);
		this.eventLog.setEvent_total(0);
		this.eventLog.setWrite_total(0);
		this.eventLog.setExce_total(0);
		this.eventLog.setApp_status(AppStatus.APP_WAITTING);
		this.eventLog.setApp_progress("0%");
		}
	}
	
	/**
	 *  记录log
	 */
	private void noteLog(Event event) {
		this.currentEvent = event;
		if (event != null) {
			HDEvent evt = (HDEvent)event;
			if (evt.metadata.get("BatchUUID") == null) {
				this.eventLog.setBatch_uuid(UUID.genCurTimeUUID().getUUIDString());
			} else {
				this.eventLog.setBatch_uuid(evt.metadata.get("BatchUUID").toString());
			}
			try {
				dataCount = Integer.parseInt(evt.metadata.get("DataCount").toString());
			} catch (Exception e) {
				dataCount = -1;
			}
		}
		if(!updating && eventLog.getEvent_starttime() == null) {
			this.eventLog.setEvent_starttime(new Timestamp(System.currentTimeMillis()));
			updating = true;
		}
	}

	public void close() throws Exception {
		if (this.pool != null) {
			this.pool.shutdown();
			this.pool.purge();
		}
//		if (this.exceptionLogUtils != null) this.exceptionLogUtils.close();
		this.closeCalled = true;
		try {
			this.writer.closeDatabaseWriter();
		} catch (Exception e) {
			throw new SystemException("Error in closing DatabaseWriter", (Throwable) e);
		}
	}

	@Override
	public void update(final Observable o, final Object arg) {
		if (arg instanceof PathManager) {
			this.lastAckedPosition = ((PathManager) arg).toPosition();
			if (this.receiptCallback != null) {
				this.receiptCallback.ack((int) this.writer.eventsInLastCommit(), ((PathManager) arg).toPosition());
			}
		} else if (arg instanceof Position) {
			this.lastAckedPosition = (Position) arg;
			if (this.receiptCallback != null) {
				this.receiptCallback.ack(1, (Position) arg);
			}
		} else if (arg instanceof ExceptionData) {
			final ExceptionData expData = (ExceptionData) arg;
			SQLException sqlExp = null;
			if (expData.exception instanceof SQLException) {
				sqlExp = (SQLException) expData.exception;
			} else if (expData.exception instanceof Exception) {
				final Throwable cause = expData.exception.getCause();
				if (cause instanceof SQLException) {
					sqlExp = (SQLException) cause;
				}
			} else {
				logger.warn("Exception type is not handled", (Throwable) expData.exception);
			}
			if (sqlExp != null) {
				final int vendorExceptionCode = sqlExp.getErrorCode();
				String ecode = "";
				if (this.isPostgres) {
					ecode = sqlExp.getSQLState();
				} else {
					ecode = "" + vendorExceptionCode;
				}
				if (!this.ignorableExceptionCode.containsKey(ecode)) {
					logger.error("Exception code {" + ecode
							+ "} is not in ignore list, throwing system exception to crash the application");
					System.out.println(sqlExp.getMessage());
					sqlExp.printStackTrace();
					try {
						Exception nextExp = sqlExp.getNextException();
						if (nextExp != null) {
							final String expMsg = nextExp.getMessage();
							final String originalExpMsg = sqlExp.getMessage();
							final String newExpMsg = originalExpMsg + "{ ExceptionCode : " + ecode + " - " + expMsg + "}";
							final String sqlState = sqlExp.getSQLState();
							final SQLException sExp = new SQLException(newExpMsg, sqlState, vendorExceptionCode);
							expData.exception = sExp;
						} else if (vendorExceptionCode == 0) {
							final String originalExpMsg2 = sqlExp.getMessage();
							final String newExpMsg2 = "{ ExceptionCode : " + ecode + " - " + originalExpMsg2 + "}";
							final String sqlState2 = sqlExp.getSQLState();
							final SQLException sExp2 = new SQLException(newExpMsg2, sqlState2, sqlExp);
							expData.exception = sExp2;
						}
						this.receiptCallback.notifyException(expData.exception, (Event) expData.eventCaused);
					}catch(Exception e) {
//						if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
						e.printStackTrace();
					}
					expData.ignored = false;
				} else {
					expData.ignored = true;
					++this.ignoredExceptionCount;
					final Exception nextException = sqlExp.getNextException();
					String errMsg = "Ignoring VendorExceptionCode {" + ecode
							+ "} exception as it is in ignore list. Exception Msg : ";
					if (nextException == null) {
						errMsg += sqlExp.getMessage();
					} else {
						errMsg = errMsg + sqlExp.getMessage() + "{" + nextException.getMessage() + "}";
					}
					logger.warn(errMsg);
				}
			}
		} else {
//			if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "SystemException", "Notification recevied with unhandled Object type {" + arg.getClass().toString() + "}",eventLog);
			logger.error("Notification recevied with unhandled Object type {" + arg.getClass().toString() + "}");
		}
	}

	public Position getWaitPosition() throws Exception {
		final Position pos = this.writer.getWaitPositionFromWriter(this.checkPointTableKey);
		this.waitPosition = new PathManagerExtn(new PathManager(pos));
		return pos;
	}

	public void publishMonitorEvents(final MonitorEventsCollection events) {
		if (this.writer != null) {
			events.add(MonitorEvent.Type.NUM_OF_EXCEPTIONS_IGNORED, this.ignoredExceptionCount);
			final DateTime recentCommitTime = this.writer.lastCommitTime();
			if (recentCommitTime != null) {
				final String timeStamp = recentCommitTime.toString();
				events.add(MonitorEvent.Type.LAST_COMMIT_TIME, timeStamp);
			}
			final DateTime lastBatchTime = this.writer.lastBatcheExecuteTime();
			if (lastBatchTime != null) {
				final String ts = lastBatchTime.toString();
				events.add(MonitorEvent.Type.LAST_IO_TIME, ts);
			}
			if (this.lastAckedPosition != null) {
				final StringBuilder positionString = new StringBuilder();
				final Collection<Path> paths = (Collection<Path>) this.lastAckedPosition.values();
				for (final Path path : paths) {
					if (positionString.length() != 0) {
						positionString.append("|");
					}
					positionString.append(path.getLowSourcePosition().toString());
				}
				events.add(MonitorEvent.Type.TARGET_COMMIT_POSITION, positionString.toString());
			}
			final long latency = this.writer.executionLatency();
			events.add(MonitorEvent.Type.EXTERNAL_IO_LATENCY, latency);
			events.add(MonitorEvent.Type.PROCESSED, this.writer.eventCount());
			events.add(MonitorEvent.Type.TOTAL_EVENTS_IN_LAST_IO, this.writer.batchedEventCount());
			events.add(MonitorEvent.Type.TOTAL_EVENTS_IN_LAST_COMMIT, this.writer.eventsInLastCommit());
		}
	}

	public MetaInfo.MetaObject onUpgrade(final MetaInfo.MetaObject metaObject, final String fromVersion,
			final String toVersion) throws Exception {
		final MetaInfo.Target targetMetaObj = (MetaInfo.Target) metaObject;
		final Map<String, Object> tempMap = (Map<String, Object>) MapFactory.makeCaseInsensitiveMap();
		tempMap.putAll(targetMetaObj.properties);
		final Object passwordProp = tempMap.get(Constant.PASSWORD);
		if (passwordProp instanceof Map && ((Map) passwordProp).get(Constant.ENCRYPTED) != null) {
			final Object encryptedPassword = ((Map) passwordProp).get(Constant.ENCRYPTED);
			if (encryptedPassword instanceof String) {
				final Password password = new Password();
				password.setEncrypted((String) encryptedPassword);
				tempMap.put(Constant.PASSWORD, password);
			}
		}
		targetMetaObj.properties = tempMap;
		logger.info("Property of " + targetMetaObj.adapterClassName + " is " + targetMetaObj.properties);
		final String chkPntTbl = (String) targetMetaObj.properties.get(Constant.CHECK_POINT_TABLE);
		if (chkPntTbl == null) {
			if (logger.isInfoEnabled()) {
				logger.info("CheckpointTable property not found in property map, setting it to default to support upgrade");
			}
			targetMetaObj.properties.put(Constant.CHECK_POINT_TABLE, "CHKPOINT");
		}
		final String commitPolicy = (String) targetMetaObj.properties.get(PolicyFactory.COMMIT_POLICY);
		if (commitPolicy == null || commitPolicy.isEmpty()) {
			if (logger.isInfoEnabled()) {
				logger.info("Commitpolicy is not specified, setting to default value");
			}
			targetMetaObj.properties.put(PolicyFactory.COMMIT_POLICY, PolicyFactory.DEFAULT_COMMIT_POLICY);
		}
		logger.info("Modified property " + targetMetaObj.properties);
		return (MetaInfo.MetaObject) targetMetaObj;
	}

	public void onCompile(final Compiler compiler, final MetaInfo.MetaObject metaObject) {
		final MetaInfo.Target targetMetaInfo = (MetaInfo.Target) metaObject;
		final Validator propValidator = new Validator(targetMetaInfo.properties);
		if (!propValidator.validate()) {
			compiler.error(propValidator.getMessage(), (Object) null);
		}
	}

	public static void main(final String[] args) throws Exception {
		final Event[] events = new Event[3];
		final DatabaseWriter_1_0 dbwr = new DatabaseWriter_1_0();
		final Map<String, Object> prop = new TreeMap<String, Object>();
		prop.put("ConnectionURL", "jdbc:oracle:thin:@//192.168.1.2:1521/orcl");
		prop.put("Username", "tpcc");
		prop.put("Password", "tpcc");
		prop.put("BatchPolicy", "Eventcount:10000,Interval:6000");
		prop.put("CommitPolicy", "Eventcount:10000,Interval:60000");
		prop.put("Tables", "DDLTEST.%,DDLTEST.%");
		prop.put("CheckpointTable", "QATEST.CHKPOINT");
		prop.put("TargetUUID", "abc");
		dbwr.init(prop, new TreeMap<String, Object>(), new UUID(123456789L), null);
		final HDEvent insertEvent = new HDEvent(3, (UUID) null);
		(insertEvent.metadata = new HashMap()).put("TimeStamp", new DateTime());
		insertEvent.metadata.put("TableName", "DDLTEST.TEST");
		insertEvent.metadata.put("OperationName", "Insert");
		insertEvent.data = new Object[3];
		insertEvent.setData(0, (Object) 1);
		insertEvent.setData(1, (Object) "Arul Anand J");
		insertEvent.setData(2, (Object) 42);
		final HDEvent updateEvent = new HDEvent(3, (UUID) null);
		(updateEvent.metadata = new HashMap()).put("TimeStamp", new DateTime());
		updateEvent.metadata.put("TableName", "DDLTEST.TEST");
		updateEvent.metadata.put("OperationName", "Update");
		updateEvent.data = new Object[3];
		updateEvent.before = new Object[3];
		updateEvent.setData(0, (Object) 1);
		updateEvent.setData(1, (Object) "arul anand j");
		updateEvent.setBefore(0, (Object) 1);
		final HDEvent deleteEvent = new HDEvent(3, (UUID) null);
		(deleteEvent.metadata = new HashMap()).put("TimeStamp", new DateTime());
		deleteEvent.metadata.put("TableName", "DDLTEST.TEST");
		deleteEvent.metadata.put("OperationName", "Delete");
		deleteEvent.data = new Object[3];
		deleteEvent.setData(0, (Object) 1);
		events[0] = (Event) insertEvent;
		events[1] = (Event) updateEvent;
		events[2] = (Event) deleteEvent;
		final long startTime = System.currentTimeMillis();
		for (int itr = 0; itr < 10000; ++itr) {
			dbwr.receiveImpl(0, events[0]);
			dbwr.receiveImpl(0, events[1]);
			dbwr.receiveImpl(0, events[2]);
		}
		final long endTime = System.currentTimeMillis();
		System.out.println("Time Take {" + (endTime - startTime) + "}");
		dbwr.close();
	}

	static {
		logger = LoggerFactory.getLogger((Class) DatabaseWriter_1_0.class);
	}
	
	class SecurityAccess {
		public void disopen() {
			
		}
    }
}
