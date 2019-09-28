package com.datasphere.proc;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.classloading.WALoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.meta.MetaInfo.Stream;
import com.datasphere.runtime.meta.MetaInfo.Target;
import com.datasphere.runtime.meta.MetaInfo.Type;
import com.datasphere.security.Password;
import com.datasphere.security.WASecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.entity.EventLog;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.proc.utils.PostgresqlUtils;

@PropertyTemplate(name = "HiveWriter", type = AdapterType.target, properties = {
		@com.datasphere.anno.PropertyTemplateProperty(name = "TableName", type = String.class, required = true, defaultValue = ""),
		@com.datasphere.anno.PropertyTemplateProperty(name = "osUsername", type = String.class, required = false, defaultValue = "" ,description="操作系统用户名"),
		@com.datasphere.anno.PropertyTemplateProperty(name = "osPassword", type = Password.class, required = false, defaultValue = "",description="操作系统密码"),
		@com.datasphere.anno.PropertyTemplateProperty(name = "ConnectionString", type = String.class, required = true, defaultValue = "jdbc:hive2://127.0.0.1:10000/default") }, outputType = HDEvent.class)
public class HiveWriter extends BaseProcess{
	private static Logger logger = Logger.getLogger(HiveWriter.class);
	private String tableName = null;
	private String dbUser = null;
	private String dbPasswd = null;
	private String dbConnectionString = null;
	public static String USERNAME = "osUsername";
	public static String PASSWORD = "osPassword";
	public static String TABLE_NAME = "TableName";
	public static String CONNECTION = "ConnectionString";
	boolean isHDEvent = false;
	private Type dataType = null;
	private Connection con;
	private List<String[]> columnList = null;
	
	private UUID appUUID;
	private UUID targetUUID;
    private String appName;
	private boolean updating = false;
    private PostgresqlUtils exceptionLogUtils;
    private EventLog eventLog;
    private int totalData;
    private int totalExceptionData;
    private Event currentEvent;
    private Event lastEvent = null;
    private int flag = 0;
    
    private static PreparedStatement stat = null;
    private String insertSql = "";
	private String updateSql = "";
	private String deleteSql = "";
	
    private String currentSQL="";
	private boolean isCommited = false;
	private int commitCount = 0;
	private ScheduledThreadPoolExecutor pool = null;

	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID);
		try {
			Stream stream = (Stream) MetadataRepository.getINSTANCE().getMetaObjectByUUID(sourceUUID,
					WASecurityManager.TOKEN);
			this.dataType = ((Type) MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType,
					WASecurityManager.TOKEN));

			this.exceptionLogUtils = new PostgresqlUtils();
			this.targetUUID = (UUID) properties.get("TargetUUID");
			try {
				Target current_stream = (Target) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUUID, WASecurityManager.TOKEN);
				this.appUUID = current_stream.getCurrentApp().getUuid();
				this.appName = current_stream.getCurrentApp().getName();
			} catch (MetaDataRepositoryException e) {
				if(exceptionLogUtils != null) exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "MetaDataRepositoryException", e.getMessage(),eventLog);
				e.printStackTrace();
			}
			
			Class<?> typeClass = WALoader.get().loadClass(this.dataType.className);
			if (typeClass.getSimpleName().equals("HDEvent")) {
				this.isHDEvent = true;
			}
			validateProperties(properties);
			
			
	        eventlog();
	        
		} catch (Exception ex) {
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", ex.getMessage(),eventLog);
			throw new AdapterException("初始化 HiveWriter 发生错误: " + ex.getMessage());
		}
	}
	/**
	 * 记录事件日志
	 */
	private void eventlog() {
		/** 记录log */
//		eventLog = new EventLog();
//		eventLog.setApp_uuid(this.appUUID.getUUIDString());
//		eventLog.setApp_name(this.appName);
//		eventLog.setCluster_name(HazelcastSingleton.getClusterName());
//		eventLog.setNode_id(HazelcastSingleton.getBindingInterface());
//        eventLog.setApp_progress("0%");
//		pool = new ScheduledThreadPoolExecutor(1);
//		pool.scheduleAtFixedRate(() -> {
//			if (currentEvent == null && lastEvent != null) {
//    			/** 更新本批数据同步最终状态 */
//				if(flag >= 1) {
//					eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()-5000));
//				} else {
//					eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
//				}
//				eventLog.setWrite_total(totalData);
//				eventLog.setEvent_total(totalData);// 数据交换总量
//				eventLog.setExce_total(totalExceptionData);
//				eventLog.setApp_status(AppStatus.APP_FINISHED);
//				eventLog.setApp_progress("100%");
//				if (updating && eventLog.getEvent_starttime() != null) {
//					updating = false;
//					exceptionLogUtils.insertOrUpdate(eventLog);
//					updating = true;
//				}
//				/** 恢复初始化状态 */
//				lastEvent = null;
//				totalData = 0;
//				updating = false;
//				eventLog.setEvent_starttime(null);
//				eventLog.setEvent_endtime(null);
//				eventLog.setEvent_total(0);
//				eventLog.setWrite_total(0);
//				eventLog.setExce_total(0);
//				eventLog.setApp_status(AppStatus.APP_WAITTING);
//				eventLog.setApp_progress("0%");
//			} else {
//				if (updating && eventLog.getEvent_starttime() != null) {
//					exceptionLogUtils.insertOrUpdate(eventLog); 
//				}
//			}
//	    		++flag;
//		}, 100, 5 * 1000, TimeUnit.MILLISECONDS);
	}
	
	private void validateProperties(Map<String, Object> properties) throws Exception {
		if ((properties.get(TABLE_NAME) != null) && (!properties.get(TABLE_NAME).toString().trim().isEmpty())) {
			this.tableName = properties.get(TABLE_NAME).toString().trim();
		} else {
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "IllegalArgumentException", "没有填写表名",eventLog);
			throw new IllegalArgumentException("必须填写表名");
		}
		if ((properties.containsKey(CONNECTION)) && (properties.get(CONNECTION) != null)) {
			this.dbConnectionString = properties.get(CONNECTION).toString();
//			System.out.println("连接字符串"+this.dbConnectionString);
		}
		if ((properties.get(USERNAME) != null) && (!properties.get(USERNAME).toString().trim().isEmpty())) {
			this.dbUser = properties.get(USERNAME).toString();
		}else {
			this.dbUser = "";
		}
		if ((properties.get(PASSWORD) != null) && (!((Password)properties.get(PASSWORD)).getPlain().isEmpty())) {
			this.dbPasswd = ((Password)properties.get(PASSWORD)).getPlain();
		}else {
			this.dbPasswd = "";
		}
		open();
	}

	
	public void open() throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		con = DriverManager.getConnection(this.dbConnectionString,this.dbUser, this.dbPasswd);

		boolean tableFound = false;
		List<String> listTableNames = new ArrayList<>();
		try (final Statement stmt = con.createStatement()) {
			final ResultSet rs = stmt.executeQuery("SHOW TABLES");
			while (rs.next()) {
				listTableNames.add(rs.getString(1));
				if (rs.getString(1).equals(this.tableName.trim())) {
					tableFound = true;
					break;
				}
			}
		}
		StringBuilder query = new StringBuilder();
		StringBuilder queryDel = new StringBuilder();
		StringBuilder queryUpdate = new StringBuilder();
		StringBuilder queryUpdateWhere = new StringBuilder();

		query.append("INSERT INTO " + this.tableName + " VALUES (");
		queryDel.append("DELETE FROM " + this.tableName + " WHERE  ");
		queryUpdate.append("UPDATE " + this.tableName + " SET ");
		queryUpdateWhere.append(" WHERE ");

		columnList = new ArrayList<>();
		int i = 0;
		int j = 0;
		if (tableFound) {
			String values = "";
			int cloumnsCount = 0;
			try (final Statement stmt = con.createStatement()) {
				final ResultSet rs = stmt.executeQuery("DESCRIBE " + this.tableName);
				while (rs.next()) {
					columnList.add(new String[] { rs.getString(1), rs.getString(2) });
					
					values += "?,";
					if (j > 0) {
						queryDel.append(" AND ");
					}
					j = 1;
					queryDel.append(rs.getString(1) + "=? ");
					if (i > 0) {
						queryUpdateWhere.append(" AND ");
					}
					i = 1;
					queryUpdateWhere.append(rs.getString(1) + "=? ");
					
					queryUpdate.append(rs.getString(1) + "=?,");
					cloumnsCount++;
				}
				if (cloumnsCount > 0) {
					values = values.substring(0, values.length() - 1);
					updateSql = queryUpdate.toString().substring(0, queryUpdate.length() - 1) + queryUpdateWhere.toString();
				}
				query.append(values);
				query.append(")");
				insertSql = query.toString();
				deleteSql = queryDel.toString();
			}

		} else {
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "AdapterException", "当查询[" + this.tableName + "]表结构时发生错误. 请指定一个有效的表名称",eventLog);
			throw new AdapterException("当查询表结构时 " + this.tableName + " 时发生错误. 请指定一个有效的表名称");
		}
		if (logger.isInfoEnabled()) {
			logger.info("表结构 " + this.tableName + " 查询成功! ");
		}
	}
	
	public void close() throws Exception {
		try {
			int count = 0;
			while(isCommited) {
				try {
					count++;
					if(count>60) {
						System.out.println("Hive数据同步任务强制结束");
						break;
					}
					Thread.sleep(500);
				}catch (Exception e) {
				}
			}
			if (this.pool != null) {
				this.pool.shutdown();
				this.pool.purge();
				pool = null;
			}
			if (stat != null && !stat.isClosed()) {
				stat.close();
			}
			if(this.con!=null && !this.con.isClosed()) {
				this.con.close();
			}
		}catch(Exception e) {
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
		}
	}
	
	@Override
	public void receiveImpl(final int channel, final Event event) throws Exception {
		try {
			/** 记录log */
			this.currentEvent = event;
//			if(!updating && eventLog.getEvent_starttime() == null) {
//				this.eventLog.setEvent_starttime(new Timestamp(System.currentTimeMillis()));
//				updating = true;
//			}
			isCommited = true;
			if(event != null) {
				Event evt = convertTypedEventToHDEvent(event);
				HDEvent we = (HDEvent) evt;
				if(we.metadata.get("OperationName") != null) {
					if (we.metadata.get("OperationName").toString().equalsIgnoreCase("INSERT") || we.metadata.get("OperationName").toString().equalsIgnoreCase("SELECT")) {
						handleInsertEvent(we);
						this.lastEvent = event;
					} else if (we.metadata.get("OperationName").toString().equalsIgnoreCase("UPDATE")) {
						handleUpdateEvent(we);
						this.lastEvent = event;
					} else if (we.metadata.get("OperationName").toString().equalsIgnoreCase("DELETE")) {
						handleDeleteEvent(we);
						this.lastEvent = event;
					}
				} else if (we.metadata.get("FileName")!=null) {
					handleInsertEvent(we);
					this.lastEvent = event;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("写入数据库出错,表名为:" + this.tableName + ": " + ex.getMessage());
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", "写入数据库出错,表名为:"+ex.getMessage(),eventLog);
			throw new AdapterException("写入数据库出错,表名为:" + this.tableName + ": " + ex.getMessage());
		}
		isCommited = false;
	}

	private Event convertTypedEventToHDEvent(final Event event) {
		HDEvent waEvent = null;
		if (event instanceof HDEvent) {
			HDEvent evt = (HDEvent) event;
			if (evt.typeUUID == null) {
				evt.typeUUID = this.typeUUID;
			}
			return evt;
		} else {
			try {
				HashMap<String,Object> metadata = new HashMap<>();
				metadata.put("OperationName", "INSERT");
				JSONObject object = new JSONObject(event.toString());
				if (object.isNull("data")) {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						waEvent = new HDEvent(payloadLength, (UUID) null);
						waEvent.typeUUID = this.typeUUID;
						waEvent.data = new Object[payloadLength];
						waEvent.metadata = metadata;
						
						int i = 0;
						for (final Object o : payload) {
							waEvent.setData(i++, o);
						}
					}
				} else {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						waEvent = new HDEvent(payloadLength, (UUID) null);
						waEvent.typeUUID = this.typeUUID;
						waEvent.data = new Object[payloadLength];
						waEvent.metadata = metadata;
						int i = 0;
						for (final Object o : payload) {
							waEvent.setData(i++, o);
						}
					} else {
						JSONArray objs = (JSONArray) object.get("data");
						int payloadLength = objs.length();
						waEvent = new HDEvent(payloadLength, (UUID) null);
						waEvent.typeUUID = this.typeUUID;
						waEvent.data = new Object[payloadLength];
						waEvent.metadata = metadata;
						for (int i = 0; i < objs.length(); i++) {
							Object obj = (Object) objs.get(i);
							waEvent.setData(i, obj);
						}
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
		return (Event) waEvent;
	}
	
	private String filterStr(String str) {
		if (str.indexOf("(") > -1) {
			str = str.substring(0, str.indexOf("("));
		}
		return str;
	}
	
	private void handleInsertEvent(HDEvent event) throws Exception {
		if(((HDEvent) event).data.length >= columnList.size()) {
			if(!currentSQL.equalsIgnoreCase(insertSql)){
				execute();
				currentSQL = insertSql;
				stat = con.prepareStatement(insertSql);
			}
			for (int i = 0; i < ((HDEvent) event).data.length; i++) {
				++totalData;
				String[] colStr = (String[]) columnList.get(i);
	
				if (BuiltInFunc.IS_PRESENT((HDEvent) event, ((HDEvent) event).data, i)) {
					addFieldValue(i, colStr[1], ((HDEvent) event).data[i], 
							new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
				}
			}
			commitCount = 0;
			stat.executeUpdate();
		} else {
			logger.error("源数据中字段数不能少于目标中字段数！");
			exceptionLogUtils.addException(this.appUUID.getUUIDString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", "源数据中字段数不能少于目标中字段数！",eventLog);
			throw new AdapterException("源数据中字段数不能少于目标中字段数！");
		}
	}
	
	private void handleUpdateEvent(HDEvent event) throws Exception {
		if(!currentSQL.equalsIgnoreCase(updateSql)){
			execute();
			currentSQL = updateSql;
			stat = con.prepareStatement(updateSql);
		}
		int noOfcolumn = 0;
		for (int i = 0; i < ((HDEvent) event).data.length; i++) {
			++totalData;
			if (BuiltInFunc.IS_PRESENT((HDEvent) event, ((HDEvent) event).data, i)) {
				String[] colStr = (String[]) columnList.get(i);
				addFieldValue(noOfcolumn, colStr[1], ((HDEvent) event).data[i], 
						new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));

			}
		}
		if (((HDEvent) event).before != null) {
			for (int i = 0; i <  ((HDEvent) event).before.length; i++) {
				if (BuiltInFunc.IS_PRESENT((HDEvent) event, ((HDEvent) event).before, i)) {
					String[] colStr = (String[]) columnList.get(i);

						addFieldValue(noOfcolumn, colStr[1], ((HDEvent) event).before[i], 
								new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
					noOfcolumn++;

				}
			}
		}
		commitCount = 0;
		stat.executeUpdate();
	}

	private void handleDeleteEvent(HDEvent event) throws Exception {
		if(!currentSQL.equalsIgnoreCase(deleteSql)){
			execute();
			currentSQL = deleteSql;
			stat = con.prepareStatement(deleteSql);
		}
		for (int i = 0; i < ((HDEvent) event).data.length; i++) {
			++totalData;
			if (BuiltInFunc.IS_PRESENT((HDEvent) event, ((HDEvent) event).data, i)) {
				String[] colStr = (String[]) columnList.get(i);
					addFieldValue(i, colStr[1], ((HDEvent) event).data[i], 
							new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
			}
		}
		commitCount = 0;
		stat.executeUpdate();
	}

	public void addFieldValue(int idx, String fieldType, Object data, 
			SimpleDateFormat inputDateFormat) throws Exception {
		int index = idx + 1;
		String type = filterStr(fieldType);
		switch (type.toUpperCase()) {
		case "CHAR":
		case "VARCHAR2":
		case "VARCHAR":
		case "STRING":
		case "CLOB":
		case "TEXT":
		case "NTEXT":
		case "NCHAR":
		case "UNIQUEIDENTIFIER":
		case "GRAPHIC":
		case "CHARACTER":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setNull(index, java.sql.Types.VARCHAR);
			} else {
				stat.setString(index, data.toString());
			}
			break;
		case "BIT":
		case "BOOLEAN":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setNull(index, java.sql.Types.BOOLEAN);
			} else {
				stat.setBoolean(index, (boolean)data);
			}
			break;
		case "NUMBER":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))) {
				stat.setInt(index, 0);
			} else {
				if ((data instanceof Byte)) {
					stat.setByte(index, ((Byte) data).byteValue());
				} else if ((data instanceof Short)) {
					stat.setShort(index, ((Short) data).shortValue());
				} else if ((data instanceof Integer)) {
					stat.setInt(index, ((Integer) data).intValue());
				} else if ((data instanceof Long)) {
					stat.setLong(index, ((Long) data).longValue());
				} else {
					if (isNumber(data.toString())) {
						stat.setBigDecimal(index, new BigDecimal(data.toString()));
					} else {
						stat.setInt(index, 0);
					}
				}
			}
			break;
		case "MONEY":
		case "SMALLMONEY":
		case "DECFLOAT":
		case "DECIMAL":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setNull(index, java.sql.Types.DECIMAL);
			} else {
				int total = 10;
				int decimal = 2;
				if(fieldType.indexOf("(")>-1) {
					String[] precision = fieldType.substring(fieldType.indexOf("(")+1, fieldType.indexOf(")")).split(",");
					total = Integer.parseInt(precision[0].trim());
					decimal = Integer.parseInt(precision[1].trim());
				}
				double f1 = new BigDecimal(data.toString()).setScale(decimal, BigDecimal.ROUND_HALF_UP).doubleValue();
				stat.setBigDecimal(index, new BigDecimal(f1));
			}
			break;
		case "DATE":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00").getTime()));
			} else {
				if (isNumber(data.toString())) {
					try {
						stat.setDate(index, new java.sql.Date(Long.parseLong(data.toString())));
					} catch (Exception e) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00").getTime()));
					}
				} else {
					if (data.toString().length() == "0000-00-00T00:00:00.000+00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00.000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00.000000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else {
						if ("'T'".contains(data.toString())) {
							inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							 stat.setDate(index, new
							 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00").getTime()));
						}
					}
				}
			}
			break;
		case "TIME":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00 00:00:00").getTime()));
			} else {
				if (isNumber(data.toString())) {
					try {
						java.util.Date date = new java.util.Date(Long.parseLong(data.toString()));
						stat.setDate(index, new java.sql.Date(date.getTime()));
					} catch (Exception e) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00 00:00:00").getTime()));
					}
				} else {
					if (data.toString().length() == "0000-00-00T00:00:00.000+00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00.000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00 00:00:00.000000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "0000-00-00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						 stat.setDate(index, new
						 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else {
						if ("'T'".contains(data.toString())) {
							inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							 stat.setDate(index, new
							 java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							stat.setDate(index, new java.sql.Date(sdf.parse("0000-00-00 00:00:00").getTime()));
						}
					}
				}
			}
			break;
		case "TIMESTAMP":
			
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setTimestamp(index, new java.sql.Timestamp(0));
			} else {
				if (data.toString().length() == "0000-00-00T00:00:00.000+00:00".length()) {
					inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				} else if (data.toString().length() == "0000-00-00 00:00:00".length()) {
					inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				} else if (data.toString().length() == "0000-00-00 00:00:00.000".length()) {
					inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				} else if (data.toString().length() == "0000-00-00 00:00:00.000000".length()) {
					inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
				} else if (data.toString().length() == "0000-00-00".length()) {
					inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				} else {
					if ("'T'".contains(data.toString())) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					} else {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						data= "0000-00-00 00:00:00";
					}
				}
				stat.setLong(index, inputDateFormat.parse(data.toString()).getTime());
			}
			break;
		case "FLOAT":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setFloat(index, 0f);
			} else {
				stat.setFloat(index, Float.parseFloat(data.toString()));
			}
			break;
		case "DOUBLE":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setDouble(index, 0f);
			} else {
				stat.setDouble(index, Double.parseDouble(data.toString()));
			}
			break;
		case "BIGINT":
		case "LONG":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setLong(index, 0l);
			} else {
				stat.setLong(index,Long.parseLong(data.toString()));
			}
			break;
		case "TINYINT":
		case "SMALLINT":
		case "INTEGER":
		case "INT":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setInt(index, 0);
			} else {
				stat.setInt(index,Integer.parseInt(data.toString()));
			}
			break;
		case "BINARY":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))) {
				InputStream in = new ByteArrayInputStream(toByteArray(""));
				stat.setBinaryStream(index, in, in.available());
			} else {
				InputStream in = new ByteArrayInputStream(toByteArray(data));
				stat.setBinaryStream(index, in, in.available());
			}
			break;

		case "REAL":
			if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
				stat.setFloat(index,0f);
			} else {
				stat.setFloat(index ,Float.parseFloat(data.toString()));
			}
			break;
		default:
				if(type.toUpperCase().startsWith("INT")) {
					if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
						stat.setInt(index, 0);
					} else {
						stat.setInt(index,Integer.parseInt(data.toString()));
					}
				}else if(type.toUpperCase().startsWith("FLOAT")) {
					if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
						stat.setFloat(index, 0f);
					} else {
						stat.setFloat(index, Float.parseFloat(data.toString()));
					}
				}else {
					if ((data == null) || (data.equals("null")) || (data.equals("NULL"))) {
						stat.setNull(index, java.sql.Types.VARCHAR);
					} else {
						stat.setString(index, data.toString());
					}
				}
		}
	}
	
	/**
	 * 对象转数组
	 * 
	 * @param obj
	 * @return
	 */
	public byte[] toByteArray(Object obj) {
		byte[] bytes = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.flush();
			bytes = bos.toByteArray();
			oos.close();
			bos.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return bytes;
	}
	
	/**
	 * 判断是否为数字
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isNumber(String str) {
		String reg = "^[0-9]+(.[0-9]+)?$";
		return str.matches(reg);
	}
	
	private void execute() {
		try {
			if(currentSQL.length() > 0 && stat != null) {
				isCommited = true;
//				stat.executeBatch();
//				stat.clearBatch();
				stat.executeUpdate();
				stat.close();
				stat = null;
				isCommited = false;
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.11.36:10000/default","", "");

		try (final Statement stmt = con.createStatement()) {
			final ResultSet rs = stmt.executeQuery("SHOW TABLES");
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
			if(rs != null)rs.close();
			Date date = new Date();
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String sql = "insert into zhou_m values ('ttt',999,'xxx','"+sdf.format(date)+"')";
			stmt.executeUpdate(sql);
			date = new Date();
			
			sql = "insert into zhou_m values ('sss',888,'yyy','"+sdf.format(date)+"')";
			stmt.executeUpdate(sql);
			sql = "insert into zhou_m values ('sss',888,'yyy','"+sdf.format(date)+"')";
			stmt.executeUpdate(sql);
			stmt.close();
			
		}
		con.close();
//		HashMap<String, Object> writerProperties = new HashMap<>();
//		writerProperties.put("ConnectionString", "jdbc:hive2://117.107.241.79:10000/default");
//		writerProperties.put(TABLE_NAME, "target_test");
//		writerProperties.put(USERNAME, "root");
//		writerProperties.put(PASSWORD, "Y8OWy26mKpAYAx4m");
//		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//		Date date = formatter.parse("2018-12-07T22:17:45.083+08:00");
//		
//		Timestamp ts = new java.sql.Timestamp(formatter.parse("2018-12-07T22:17:45.083+08:00").getTime());
//		
//		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
//		String sDate=sdf.format(ts);
//		System.out.println(sDate);
		
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
//		Date date = sdf.parse("2018-12-08 19:00:56.094466");
//		date.getTime();
//		try {
//			HiveWriter dbWriter = new HiveWriter();
//			dbWriter.validateProperties(writerProperties);
//			dbWriter.open();
//			dbWriter.insertToDatabase("insert into t1 values ('3','niubi222','24', '2018-11-27 20:46:30')");
//			dbWriter.close();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
		
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
