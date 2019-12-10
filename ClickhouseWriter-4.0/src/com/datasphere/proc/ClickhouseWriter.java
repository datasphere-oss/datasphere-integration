package com.datasphere.proc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.appmanager.AppManager;
import com.datasphere.classloading.HDLoader;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.meta.MetaInfo.Stream;
import com.datasphere.runtime.meta.MetaInfo.Type;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.entity.TempRecord;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "ClickhouseWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = "jdbc:clickhouse://127.0.0.1:9000/default"),
		@PropertyTemplateProperty(name = "Username", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "500"),
		@PropertyTemplateProperty(name = "Interval", type = Integer.class, required = false, defaultValue = "1"),
		@PropertyTemplateProperty(name = "TimeField", type = String.class, required = false, defaultValue = "") }, outputType = HDEvent.class)
public class ClickhouseWriter extends BaseProcess {

	private static Logger logger = LoggerFactory.getLogger(ClickhouseWriter.class);
	private Connection con;
	private String url;
	private String username;
	private String password;
	private String tables;
	private String timeField = null;
	private int timeFieldIndex = -1;
	private String timeFieldValue = null;

	private PreparedStatement insertStat = null;
	private PreparedStatement updateStat = null;
	private PreparedStatement deleteStat = null;
	private PreparedStatement deleteByKeyStat = null;

	private boolean isHDEvent = false;
	private Type dataType = null;
	private List<String[]> columnList = null;
	private List<Integer> defaultValueIndexList = new ArrayList<>();
	private List<String> keyList = new ArrayList<>();
	private List<Integer> keyIndexList = new ArrayList<>();
	private String insertSql = "";
	private String updateSql = "";
	private String deleteSql = "";
	private String deleteSqlByKey = "";

	private boolean isMergeTree = false;
	private HashMap<String, Object> metadata = null;
	/**
	 * 0:插入 1:更新 2：删除 -1：其它
	 */
	private int status = 0;
	/**
	 * 上一次状态
	 */
	private int preStatus = 0;
	private boolean isCommited = false;
	private int commitCount = 0;
	private int insertRows = 0;
	private int fetchSize = 5000;
	private int totalReciveDataCount = 0;//任务启动至停止时累计同步数据量
	/**
	 * 提交间隔，秒
	 */
	private int interval = 1 * 1000;
	private ScheduledThreadPoolExecutor pool = null;
	private boolean updating = true;
	private static final String SIMPLE_NAME = "HDEvent";
	private static final String USER_NAME = "Username";
	private static final String PASSWORD = "Password";
	private static final String FETCH_SIZE = "FetchSize";

	private LinkedBlockingQueue<HDEvent> insertQueue = null;
	private LinkedBlockingQueue<HDEvent> updateQueue = null;
	private LinkedBlockingQueue<HDEvent> deleteQueue = null;

	private LinkedBlockingQueue<HDEvent> commitQueue = null;

	Thread insertThread = null;
	Thread updateThread = null;
	Thread deleteThread = null;
	private UUID targetUUID = null;
	private com.datasphere.runtime.components.Target target = null;
	private String appName = null;
	private String lastValue = null;
	private String moreWhere = null;

	boolean tableFound = false;
	
	/**
	 * 初始化
	 */
	@Override
	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID);
		this.targetUUID = (UUID) properties.get("TargetUUID");
		target = (com.datasphere.runtime.components.Target) this.receiptCallback;
		Stream stream = (Stream) MetadataRepository.getINSTANCE().getMetaObjectByUUID(sourceUUID,
				HDSecurityManager.TOKEN);
		this.dataType = ((Type) MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType,
				HDSecurityManager.TOKEN));
		this.appName =  stream.getCurrentApp().getName();
		this.totalReciveDataCount = 0;
		Class<?> typeClass = HDLoader.get().loadClass(this.dataType.className);
		if (typeClass.getSimpleName().equals("HDEvent")) {
			this.isHDEvent = true;
		}

		if (!properties.containsKey(USER_NAME) || properties.get(USER_NAME) == null
				|| properties.get(USER_NAME).toString().length() <= 0) {
			this.username = "";
		} else {
			this.username = properties.get(USER_NAME).toString();
		}
		if (!properties.containsKey(PASSWORD) || properties.get(PASSWORD) == null
				|| ((Password) properties.get(PASSWORD)).getPlain().toString().length() <= 0) {
			this.password = "";
		} else {
			this.password = ((Password) properties.get(PASSWORD)).getPlain().toString();
		}
		try {
			int fetchS = 500;
			if (properties.containsKey(FETCH_SIZE) && properties.get(FETCH_SIZE) != null) {
				Object val = properties.get(FETCH_SIZE);
				if (val instanceof Number) {
					fetchS = ((Number) val).intValue();
				} else if (val instanceof String) {
					fetchS = Integer.parseInt((String) val);
				}
				if (fetchS < 0 || fetchS > 1000000000) {
					fetchS = 100;
				}
			} else {
				fetchS = 500;
			}
			this.fetchSize = fetchS;
		} catch (Exception e3) {
			this.fetchSize = 500;
		}

		if (properties.containsKey("Interval") && properties.get("Interval") != null
				&& properties.get("Interval").toString().length() > 0) {
			this.interval = Integer.parseInt(properties.get("Interval").toString()) * 1000;
		}
		if (properties.get("TimeField") != null) {
			this.timeField = properties.get("TimeField").toString();
		}

		this.url = properties.get("ConnectionURL").toString();

		String tbl = properties.get("Tables").toString().trim();

		if (tbl.contains(";")) {
			tbl = tbl.substring(0, tbl.indexOf(";"));
		}
		if (tbl.contains(".") && !tbl.endsWith(".")) {
			this.tables = tbl.substring(tbl.indexOf(".") + 1);
		} else {
			this.tables = tbl;
		}
		(this.metadata = new HashMap<String, Object>()).put("TableName", dataType.name);
		this.metadata.put("OperationName", "INSERT");
		this.updating = true;
		try {
			Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
			con = DriverManager.getConnection(this.url, this.username, this.password);
			// con.setAutoCommit(false);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AdapterException("连接数据库出错,请检查数据库连接信息，错误详情：" + ex.getMessage());
		}

		
		createSql();

		

		insertQueue = new LinkedBlockingQueue<>();
		updateQueue = new LinkedBlockingQueue<>();
		deleteQueue = new LinkedBlockingQueue<>();
		commitQueue = new LinkedBlockingQueue<>();

		insertThread = new Thread() {
			@Override
			public void run() {
				try {
					insertData();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		};
		updateThread = new Thread() {
			@Override
			public void run() {
				try {
					updateData();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		};
		deleteThread = new Thread() {
			@Override
			public void run() {
				try {
					deleteData();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		};
		insertThread.start();
		updateThread.start();
		deleteThread.start();

		if (this.interval < 100) {
			this.interval *= 1000;
		}
		autoCommit();
	}

	/**
	 * 停止任务
	 */
	@Override
	public void close() throws Exception {
		insertThread.interrupt();
		updateThread.interrupt();
		deleteThread.interrupt();
		insertQueue.clear();
		updateQueue.clear();
		deleteQueue.clear();
		insertThread = null;
		updateThread = null;
		deleteThread = null;
		insertQueue = null;
		updateQueue = null;
		deleteQueue = null;
		commitQueue = null;

		if (this.pool != null) {
			this.pool.shutdown();
			this.pool.purge();
			pool = null;
		}
		if (insertStat != null && !insertStat.isClosed()) {
			insertStat.close();
		}
		if (updateStat != null && !updateStat.isClosed()) {
			updateStat.close();
		}
		if (deleteStat != null && !deleteStat.isClosed()) {
			deleteStat.close();
		}
		if (con != null && !con.isClosed()) {
			con.close();
		}
	}

	/**
	 * 生成 insert 语句
	 * 
	 * @throws Exception
	 */
	public void createSql() throws Exception {

		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("SHOW TABLES");
		while (rs.next()) {
			if (rs.getString(1).equalsIgnoreCase(this.tables.trim())) {
				tableFound = true;
				break;
			}
		}
		if (rs != null) {
			rs.close();
			rs = null;
		}
		rs = stmt.executeQuery("SHOW CREATE TABLE " + this.tables);
		while (rs.next()) {
			String sql = rs.getString(1);
			String[] engine = sql.toUpperCase().split("ENGINE");
			if (engine.length > 1) {
				if (engine[1].toUpperCase().indexOf("MERGETREE") > -1) {
					isMergeTree = true;
					tableFound = true;
					try {
						keyList = ParserMergeTree.parser(sql);
					} catch (Exception e) {

					}
				}
			}
		}
		if (rs != null) {
			rs.close();
			rs = null;
		}
		if (this.timeField != null) {
			rs = stmt.executeQuery("SELECT max(" + this.timeField + ") FROM " + this.tables);
			if (rs.next()) {
				this.timeFieldValue = rs.getObject(1) == null ? null : rs.getObject(1) + "";
			}
		}
		if (rs != null) {
			rs.close();
			rs = null;
		}
		if(stmt != null) {
			stmt.close();
			stmt = null;
		}
		
	}
	
	private void initSQL(int dataLength) throws SQLException, AdapterException {
		Statement stmt = con.createStatement();
		StringBuilder query = new StringBuilder();
		StringBuilder queryDel = new StringBuilder();
		StringBuilder queryUpdate = new StringBuilder();
		StringBuilder queryUpdateWhere = new StringBuilder();
		query.append("INSERT INTO " + this.tables );
		queryDel.append("DELETE FROM " + this.tables + " WHERE  ");
		queryUpdate.append("ALTER TABLE  " + this.tables + " UPDATE " + " ");
		queryUpdateWhere.append(" WHERE ");
		columnList = new ArrayList<String[]>();
		defaultValueIndexList = new ArrayList<>();
		int i = 0;
		int j = 0;
		int k = 0;
		if (tableFound) {
			final ResultSet rs2 = stmt.executeQuery("DESCRIBE TABLE " + this.tables);
			String values = "";
			String cloumnNames = "";
			int cloumnsCount = 0;
			while (rs2.next()) {
				if(cloumnsCount >= dataLength) {
					break;
				}
				if (this.timeField != null && this.timeField.equalsIgnoreCase(rs2.getString(1))) {
					this.timeFieldIndex = cloumnsCount;
				}
				columnList.add(new String[] { rs2.getString(1), rs2.getString(2), rs2.getString(3), rs2.getString(4) });
				if ("DEFAULT".equalsIgnoreCase(rs2.getString(3))) {
					defaultValueIndexList.add(cloumnsCount);
				}
				cloumnNames += rs2.getString(1) + ",";
				values += "?,";
				if (j > 0) {
					queryDel.append(" AND ");
				}
				j = 1;
				queryDel.append(rs2.getString(1) + "=? ");
				if (i > 0) {
					queryUpdateWhere.append(" AND ");
				}
				queryUpdateWhere.append(rs2.getString(1) + "=? ");
				i = 1;
				if (isPrimarykey(rs2.getString(1))) {
					keyIndexList.add(cloumnsCount);
					if (k > 0) {
						deleteSqlByKey += " AND ";
					}
					k = 1;
					deleteSqlByKey += rs2.getString(1) + "=? ";
				} else {
					queryUpdate.append(rs2.getString(1) + "=?,");
				}
				cloumnsCount++;
			}
			if (cloumnsCount > 0) {
				values = values.substring(0, values.length() - 1);
				cloumnNames = cloumnNames.substring(0,cloumnNames.length() - 1);
				updateSql = queryUpdate.toString().substring(0, queryUpdate.length() - 1) + queryUpdateWhere.toString();
			}
			query.append(" (" + cloumnNames + ") VALUES (" + values + ")");
			insertSql = query.toString();
			deleteSql = queryDel.toString();
			if (keyIndexList.size() > 0) {
				deleteSqlByKey = "DELETE FROM " + this.tables + " WHERE  " + deleteSqlByKey;
			}
			insertStat = con.prepareStatement(insertSql);
			updateStat = con.prepareStatement(updateSql);
			deleteStat = con.prepareStatement(deleteSql);
			deleteByKeyStat = con.prepareStatement(deleteSqlByKey);
//			System.out.println("-----构造插入语句："+insertSql);
		} else {
			throw new AdapterException("当查询表结构时 " + this.tables + " 时发生错误. 请指定一个有效的表名称");
		}
	}

	/**
	 * 判断是否是主键
	 * 
	 * @param cloumnName
	 * @return
	 */
	private boolean isPrimarykey(String cloumnName) {
		if (keyList.contains(cloumnName)) {
			return true;
		}
		return false;
	}

	/**
	 * 定时提交
	 */
	private void autoCommit() {
		if (pool == null) {
			ThreadFactory namedThreadFactory = (Runnable r) -> new Thread(r,
					"thread_pool_UltraDBWriter_" + r.hashCode());
			pool = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
			pool.scheduleAtFixedRate(() -> {
				try {
					// System.out.println(insertRows+"-----con:" + (con!=null) + " isCommited:" +
					// (!isCommited) + " insertStat:" + insertStat + " updating:" + this.updating);
					if (columnList != null && columnList.size() > 0 && commitCount > 2 && !isCommited && insertRows > 0) {
						commitCount = 0;
						if (con != null && insertStat != null && this.updating) {
							this.updating = false;
							isCommited = true;
							insertRows = 0;
							insertStat.executeBatch();
							con.commit();
							insertStat.clearBatch();
							isCommited = false;
							this.updating = true;
							System.out.println("----定时执行插入");
						}
					}
					commitCount++;
				} catch (SQLException e) {
					// reInsert();
					e.printStackTrace();
				}
			}, 5000, this.interval, TimeUnit.MILLISECONDS);
		}
	}

	private void insertData() throws Exception {
		try {
			HDEvent evt = null;
			while ((evt = insertQueue.take()) != null) {
				handleInsertEvent(evt);
			}
		} catch (Exception ex) {
			// throw new AdapterException("写入数据出错，表名为 " + this.tables + ": " +
			// ex.getMessage());
		}
	}

	private void updateData() throws Exception {
		try {
			HDEvent evt = null;
			while ((evt = updateQueue.take()) != null) {
				if (isMergeTree) {
					handleUpdateEvent(evt);
				}
			}
		} catch (Exception ex) {
			// ex.printStackTrace();
			// throw new AdapterException("更新数据出错，表名为 " + this.tables + ": " +
			// ex.getMessage());
		}
	}

	private void deleteData() throws Exception {
		try {
			HDEvent evt = null;
			while ((evt = deleteQueue.take()) != null) {
				if (isMergeTree) {
					handleDeleteEvent(evt);
				}
			}
		} catch (Exception ex) {
			// ex.printStackTrace();
			// throw new AdapterException("删除数据出错，表名为 " + this.tables + ": " +
			// ex.getMessage());
		}
	}

	int datacount = 0;
	/**
	 * 处理接收到的消息
	 */
	@Override
	public void receiveImpl(int channel, Event event) throws Exception {
		if (event == null) {
			System.out.println("收到空数据");
			return;
		}
		if(datacount == 0) {
			datacount = 1;
//			System.out.println("收到数据："+event.toString());
		}
		// if (!this.isHDEvent) {
		event = convertTypedEventToHDEvent(event);
		// }
		HDEvent evt = (HDEvent) event;
		if(columnList == null || columnList.size() == 0) {
			initSQL(evt.data.length);
		}
		if((evt != null && (evt.metadata.containsKey("status") && "Completed".equalsIgnoreCase(evt.metadata.get("status").toString())))) {
			stopApp(evt);
			return;
		}
		if (this.timeFieldValue != null && this.timeFieldIndex > -1 && evt.data.length > this.timeFieldIndex
				&& evt.data[this.timeFieldIndex] != null) {
			String date = evt.data[this.timeFieldIndex].toString();
			if (date.length() > this.timeFieldValue.length()) {
				date = date.substring(0, this.timeFieldValue.length());
			}
			if (date.compareTo(this.timeFieldValue) <= 0) {
				System.out.println("----时间字段[" + this.timeField + "]值已经存在：" + this.timeFieldValue + ",当前数据值：" + date);
				return;
			}
		}
		totalReciveDataCount++;
		String operationName = evt.metadata.get(Constant.OPERATION) != null
				? evt.metadata.get(Constant.OPERATION).toString()
				: "INSERT";
		if ("INSERT".equalsIgnoreCase(operationName) || "SELECT".equalsIgnoreCase(operationName)) {
			insertQueue.put(evt);
		} else if ("UPDATE".equalsIgnoreCase(operationName)) {
			updateQueue.put(evt);
		} else if ("DELETE".equalsIgnoreCase(operationName)) {
			deleteQueue.put(evt);
		}

	}
	
	private boolean stopApp(HDEvent evt) {
		try {
			System.out.println("任务 " + this.appName + " 同步完成，累计同步数据量：" + totalReciveDataCount + "条，同步结束停止任务。");
			logger.info("任务 " + this.appName + " 同步完成，累计同步数据量：" + totalReciveDataCount + "条，同步结束停止任务。");
			this.updating = false;
			isCommited = true;
			insertRows = 0;
			insertStat.executeBatch();
			con.commit();
			insertStat.clearBatch();
			isCommited = false;
			this.updating = true;
			com.datasphere.runtime.meta.MetaInfo.MetaObject metaObject = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUUID, HDSecurityManager.TOKEN);
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
	 * 转换事件对象
	 * 
	 * @param event
	 * @return
	 */
	private Event convertTypedEventToHDEvent(final Event event) {
		HDEvent HDEvent = null;
		if (event instanceof HDEvent) {
			HDEvent evt = (HDEvent) event;
			if (evt.typeUUID == null) {
				evt.typeUUID = this.typeUUID;
//				evt.metadata = this.metadata;
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
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
		return (Event) HDEvent;
	}

	/**
	 * 插入数据库
	 * 
	 * @param event
	 * @throws Exception
	 */
	private void handleInsertEvent(HDEvent event) throws Exception {
		synchronized (con) {
			try {
				if (event == null || event.data == null) {
					return;
				}
				commitQueue.put(event);
				if (defaultValueIndexList.size() == 0) {
					for (int i = 0; i < event.data.length; i++) {
						if (i >= columnList.size()) {
							System.out.println("出错了,列数与数据库中列数不对应！" + columnList.size());
						}
						if (i < columnList.size()) {
							String[] colStr = (String[]) columnList.get(i);
							try {
								insertStat = addInsertFieldValue(i, colStr[0], colStr[1], event.data[i], insertStat);
							} catch (Exception e) {
								System.out.println("----无默认值，共 " + event.data.length + " 个数据，" + columnList.size()
										+ "个字段，第 " + i + " 个字段，" + colStr[0] + "," + colStr[1]);
								throw new Exception(e.getMessage());
							}
						}
					}
					commitCount = 0;
					insertStat.addBatch();
					insertRows++;
					if (insertRows >= this.fetchSize) {
						isCommited = true;
						commitCount = 0;
						this.updating = false;
						long start = System.currentTimeMillis();
						if (insertStat == null) {
							System.out.println("----PreparedStatement:为空！");
						}
						// try {
						insertStat.executeBatch();
						// }catch(NullPointerException e) {
						// System.out.println("----插入数据时发生空指针异常！");
						// e.printStackTrace();
						// try {
						// HDEvent evt = null;
						// while ((evt = commitQueue.take()) != null) {
						// System.out.println("------------------");
						// System.out.println(evt.toJSON());
						// }
						// } catch (Exception ex) {
						//
						// }
						// }catch(SQLException e) {
						// System.out.println("----发生SQL异常");
						// e.printStackTrace();
						// }catch(Exception e) {
						// System.out.println("----发生其它异常");
						// e.printStackTrace();
						// }
						commitQueue.clear();
						long end = System.currentTimeMillis();
						long timelength = (end - start) / 1000;
						if (timelength <= 0) {
							timelength = 1;
						}
						insertStat.clearBatch();
						isCommited = false;
						this.updating = true;
						try {
							System.out.println("----批量提交：" + insertRows + " 条，耗时：" + timelength + "秒,");
						} catch (Exception e) {
							e.printStackTrace();
						}
						insertRows = 0;

					}
				} else {
					for (int i = 0; i < event.data.length; i++) {
						if (i >= columnList.size()) {
							System.out.println("出错了,列数与数据库中列数不对应！");
						}

						String[] colStr = (String[]) columnList.get(i);
						if (i < event.data.length) {
							try {
								if (("DEFAULT".equalsIgnoreCase(colStr[2])
										&& (event.data[i] == null || event.data[i].toString().length() == 0))) {
//									String value = colStr[3];
//									if(value.trim().startsWith("'")) {
//										value = value.replace("'", "");
//									}
									String value = null;
									insertStat = addInsertFieldValue(i, colStr[0], colStr[1], value, insertStat);
								} else {
									insertStat = addInsertFieldValue(i, colStr[0], colStr[1], event.data[i],
											insertStat);
								}
							} catch (Exception e) {
								System.out.println("----有默认值，共 " + event.data.length + " 个数据，" + columnList.size()
										+ "个字段，第 " + i + " 个字段，" + colStr[0] + "," + colStr[1]);
								throw new Exception(e.getMessage());
							}
						}else {
							if (("DEFAULT".equalsIgnoreCase(colStr[2]))) {
								insertStat = addInsertFieldValue(i, colStr[0], colStr[1], colStr[3], insertStat);
							} else {
								insertStat = addInsertFieldValue(i, colStr[0], colStr[1], null,
										insertStat);
							}
						}
					}

					commitCount = 0;
					insertStat.addBatch();
					insertRows++;
					if (insertRows >= this.fetchSize) {
						isCommited = true;
						commitCount = 0;
						this.updating = false;
						long start = System.currentTimeMillis();
						insertStat.executeBatch();
						commitQueue.clear();
						long end = System.currentTimeMillis();
						long timelength = (end - start) / 1000;
						if (timelength <= 0) {
							timelength = 1;
						}
						try {
							System.out.println("----批量提交：" + insertRows + " 条，耗时：" + timelength + "秒,");
						} catch (Exception e) {
							e.printStackTrace();
						}
						insertStat.clearBatch();
						isCommited = false;
						this.updating = true;
						insertRows = 0;
					}
				}

			} catch (SQLException e) {
				e.printStackTrace();
				reInsert();
			} catch (Exception e) {
				e.printStackTrace();
				reInsert();
			}
		}

	}

	/**
	 * 删除之前的数据重新插入
	 */
	private void reInsert() {
		try {
			// handleDeleteByKey();
			// HDEvent evt = null;
			// while ((evt = commitQueue.poll()) != null) {
			// handleInsertEvent(evt);
			// }
			commitQueue.clear();
		} catch (Exception ex) {
		}
	}

	/**
	 * 删除数据
	 * 
	 * @param event
	 * @throws Exception
	 */
	private void handleDeleteEvent(HDEvent event) throws Exception {
		synchronized (con) {
			try {
				for (int i = 0; i < event.data.length; i++) {
					if (i >= columnList.size()) {
						System.out.println("出错了,列数与数据库中列数不对应！");
					}
					if (i < columnList.size()) {
						String[] colStr = (String[]) columnList.get(i);
						deleteStat = addInsertFieldValue(i, colStr[0], colStr[1], event.data[i], deleteStat);
					}
				}
				deleteStat.addBatch();
				insertRows++;
				if (insertRows > this.fetchSize) {
					insertRows = 0;
					isCommited = true;
					commitCount = 0;
					this.updating = false;
					long start = System.currentTimeMillis();
					deleteStat.executeBatch();
					long end = System.currentTimeMillis();
					logger.info("批量提交耗时：" + (end - start) / 1000 + "s");
					deleteStat.clearBatch();
					isCommited = false;
					this.updating = true;
				}
			} catch (SQLException sqlExp) {
				sqlExp.printStackTrace();
				logger.info(sqlExp.getMessage());
			}
		}
	}

	/**
	 * 更新数据
	 * 
	 * @param event
	 * @throws Exception
	 */
	private void handleUpdateEvent(HDEvent event) throws Exception {
		synchronized (con) {
			try {
				int j = 0;
				int i = 0;
				for (; i < event.data.length; i++) {
					if (i >= columnList.size()) {
						System.out.println("出错了,列数与数据库中列数不对应！");
					}
					if (i < columnList.size()) {
						String[] colStr = (String[]) columnList.get(i);
						if (!keyList.contains(colStr[0])) {
							updateStat = addInsertFieldValue(j++, colStr[0], colStr[1], event.data[i], updateStat);
						}
					}
				}
				for (i = j; i < event.before.length + j; i++) {
					String[] colStr = (String[]) columnList.get(i - j);
					updateStat = addInsertFieldValue(i, colStr[0], colStr[1], event.before[i - j], updateStat);
				}
				updateStat.addBatch();
				insertRows++;
				if (insertRows > this.fetchSize) {
					insertRows = 0;
					commitCount = 0;
					isCommited = true;
					this.updating = false;
					updateStat.executeBatch();
					updateStat.clearBatch();
					isCommited = false;
					this.updating = true;
				}
			} catch (SQLException sqlExp) {
				sqlExp.printStackTrace();
				logger.info(sqlExp.getMessage());
			}
		}
	}

	private void handleDeleteByKey() throws Exception {
		synchronized (con) {
			try {
				HDEvent evt = null;
				while ((evt = commitQueue.poll()) != null) {
					deleteQueue.add(evt);
					for (int i = 0; i < columnList.size(); i++) {
						if (i == keyIndexList.get(i)) {
							String[] colStr = (String[]) columnList.get(i);
							if (keyList.contains(colStr[0])) {
								deleteByKeyStat = addInsertFieldValue(i, colStr[0], colStr[1], evt.data[i], deleteStat);
							}
						}
					}
					deleteByKeyStat.addBatch();
				}
				insertRows = 0;
				isCommited = true;
				commitCount = 0;
				this.updating = false;
				deleteByKeyStat.executeBatch();
				deleteByKeyStat.clearBatch();
				isCommited = false;
				this.updating = true;
			} catch (SQLException sqlExp) {
				sqlExp.printStackTrace();
				logger.info(sqlExp.getMessage());
			}
		}
	}

	/**
	 * 组装数据
	 * 
	 * @param idx
	 * @param fieldType
	 * @param data
	 * @param inputDateFormat
	 * @throws Exception
	 */
	public PreparedStatement addInsertFieldValue(int idx, String fieldName, String fieldType, Object data,
			PreparedStatement mStat) throws Exception {
		PreparedStatement stat = mStat;
		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		int index = idx + 1;
		String type = fieldType.toUpperCase().trim();
		type = type.replace("NULLABLE(STRING)", "STRING");
		if (type.startsWith("NULLABLE(")) {
			type = type.substring(9, type.length() - 1);
		}
		if (type.indexOf("(") > -1) {
			type = type.substring(0, type.indexOf("("));
		}
		switch (type) {
		case "CHAR":
		case "NCHAR":
		case "VARCHAR2":
		case "VARCHAR":
		case "NVARCHAR2":
		case "NVARCHAR":
		case "TEXT":
		case "NTEXT":
		case "CHARACTER":
		case "BINARY_DOUBLE":
		case "BINARY_FLOAT":
		case "STRING":
		case "FIXEDSTRING":
			if (data == null || data.toString().equalsIgnoreCase("null") || data.toString().trim().length() == 0) {
				// stat.setNull(index, java.sql.Types.VARCHAR);
				stat.setString(index, "");
			} else {
				stat.setString(index, data.toString());
			}
			break;
		case "UINT8":
		case "UINT16":
		case "UINT32":
		case "UINT64":
		case "INT4":
		case "INT":
		case "INT8":
		case "INT16":
		case "INT32":
		case "INT64":
		case "NUMBER":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				// stat.setNull(index, java.sql.Types.INTEGER);
				stat.setInt(index, 0);
			} else {
				if(data.toString().contains("CAST")  || data.toString().contains("(")) {
					stat.setObject(index, data);
				} else if ((data instanceof Byte)) {
					stat.setByte(index, ((Byte) data).byteValue());
				} else if ((data instanceof Short)) {
					stat.setShort(index, ((Short) data).shortValue());
				} else if ((data instanceof Integer)) {
					stat.setInt(index, ((Integer) data).intValue());
				} else if ((data instanceof Long)) {
					stat.setLong(index, ((Long) data).longValue());
				} else {
					if (isNumber(data.toString())) {
						stat.setShort(index, Short.parseShort(data.toString()));
					} else {
						stat.setShort(index, (short) 0);
					}
				}
			}
			break;
		case "LONG":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				// stat.setNull(index, java.sql.Types.INTEGER);
				stat.setLong(index, 0l);
			} else {
				stat.setLong(index, Long.parseLong(data.toString()));
			}
			break;
		case "FLOAT32":
		case "FLOAT64":
		case "FLOAT":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				stat.setFloat(index, 0f);
			} else {
				stat.setFloat(index, Float.parseFloat(data.toString()));
			}
			break;
		case "DOUBLE":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				stat.setDouble(index, 0d);
			} else {
				stat.setDouble(index, Double.parseDouble(data.toString()));
			}
			break;
		case "DATE":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				stat.setDate(index, new java.sql.Date(sdf.parse("1970-01-01").getTime()));
			} else {
				if (isNumber(data.toString())) {
					try {
						stat.setDate(index, new java.sql.Date(Long.parseLong(data.toString())));
					} catch (Exception e) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						stat.setDate(index, new java.sql.Date(sdf.parse("1970-01-01").getTime()));
					}
				} else {
					if(data.toString().contains("CAST") || data.toString().contains("(")) {
						stat.setObject(index, data);
					}else if (data.toString().length() == "1970-01-01T00:00:00.000+00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
					} else {
						if ("'T'".contains(data.toString())) {
							inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							stat.setDate(index, new java.sql.Date(inputDateFormat.parse(data.toString()).getTime()));
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							stat.setDate(index, new java.sql.Date(sdf.parse("1970-01-01").getTime()));
						}
					}
				}
			}
			break;
		case "TIME":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				stat.setTime(index, new java.sql.Time(sdf.parse("1970-01-01 00:00:00").getTime()));
			} else {
				if (isNumber(data.toString())) {
					try {
						java.util.Date date = new java.util.Date(Long.parseLong(data.toString()));
						stat.setTime(index, new java.sql.Time(date.getTime()));
					} catch (Exception e) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setTime(index, new java.sql.Time(sdf.parse("1970-01-01 00:00:00").getTime()));
					}
				} else {
					if (data.toString().length() == "1970-01-01T00:00:00.000+00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
						stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
					} else {
						if ("'T'".contains(data.toString())) {
							inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							stat.setTime(index, new java.sql.Time(inputDateFormat.parse(data.toString()).getTime()));
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							stat.setTime(index, new java.sql.Time(sdf.parse("1970-01-01 00:00:00").getTime()));
						}
					}
				}
			}
			break;
		case "DATETIME":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				stat.setTimestamp(index, new java.sql.Timestamp(sdf.parse("1970-01-01 00:00:00").getTime()));
			} else {
				if (isNumber(data.toString())) {
					try {
						java.util.Date date = new java.util.Date(Long.parseLong(data.toString()));
						stat.setTimestamp(index, new java.sql.Timestamp(date.getTime()));
					} catch (Exception e) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setTimestamp(index, new java.sql.Timestamp(sdf.parse("1970-01-01 00:00:00").getTime()));
					}
				} else {
					if (data.toString().length() == "1970-01-01T00:00:00.000+00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						stat.setTimestamp(index,
								new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						stat.setTimestamp(index,
								new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						stat.setTimestamp(index,
								new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01 00:00:00.000000".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
						stat.setTimestamp(index,
								new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
					} else if (data.toString().length() == "1970-01-01".length()) {
						inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						stat.setTimestamp(index,
								new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
					} else {
						if ("'T'".contains(data.toString())) {
							inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							stat.setTimestamp(index,
									new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							stat.setTimestamp(index,
									new java.sql.Timestamp(sdf.parse("1970-01-01 00:00:00").getTime()));
						}
					}
				}
			}
			break;
		case "TIMESTAMP":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				// stat.setNull(index, java.sql.Types.TIMESTAMP);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				stat.setTimestamp(index, new java.sql.Timestamp(sdf.parse("1970-01-01 00:00:00").getTime()));
			} else {
				stat.setTimestamp(index, new java.sql.Timestamp(inputDateFormat.parse(data.toString()).getTime()));
			}
			break;
		case "CLOB":
		case "NCLOB":
			if (data == null || data.toString().equalsIgnoreCase("null") || data.toString().trim().length() == 0) {
				stat.setString(index, "");
			} else {
				Reader clobReader = new StringReader(data.toString());
				stat.setCharacterStream(index, clobReader, data.toString().length());
			}
			break;
		case "BLOB":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				InputStream in = new ByteArrayInputStream(toByteArray(""));
				stat.setBinaryStream(index, in, in.available());
			} else {
				InputStream in = new ByteArrayInputStream(toByteArray(data));
				stat.setBinaryStream(index, in, in.available());
			}
			break;
		case "REAL":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				stat.setFloat(index, 0f);
			} else {
				stat.setFloat(index, Float.parseFloat(data.toString()));
			}
			break;
		case "DECIMAL32":
		case "DECIMAL64":
		case "DECIMAL128":
		case "DECIMAL":
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				stat.setBigDecimal(index, new BigDecimal(0));
			} else {
				stat.setBigDecimal(index, new BigDecimal(data.toString()));
			}
			break;
		default:
			System.out.println("----未知字段类型：" + type + " 数据：" + data);
			if ((data == null) || (data.toString().equalsIgnoreCase("null"))
					|| (data.toString().trim().length() == 0)) {
				stat.setString(index, "");
			} else {
				if (data instanceof Boolean) {
					stat.setInt(index, ((boolean) data) ? 1 : 0);
				} else {
					stat.setObject(index, data);
				}
			}
		}

		return stat;
	}

	public static String returnDefaultValue(String fieldType) throws Exception {
		String type = fieldType.toUpperCase().trim();
		if (type.startsWith("NULLABLE(")) {
			type = type.substring(9, type.length() - 1);
		}
		if (type.indexOf("(") > -1) {
			type = type.substring(0, type.indexOf("("));
		}
		switch (type.toUpperCase()) {
		case "CHAR":
		case "NCHAR":
		case "VARCHAR2":
		case "VARCHAR":
		case "NVARCHAR2":
		case "NVARCHAR":
		case "TEXT":
		case "NTEXT":
		case "CHARACTER":
		case "BINARY_DOUBLE":
		case "BINARY_FLOAT":
		case "STRING":
		case "FIXEDSTRING":
			return "''";
		case "UINT8":
		case "UINT16":
		case "UINT32":
		case "UINT64":
		case "INT8":
		case "INT16":
		case "INT32":
		case "INT64":
		case "NUMBER":
			return "0";
		case "LONG":
			return "0";
		case "FLOAT8":
		case "FLOAT16":
		case "FLOAT32":
		case "FLOAT64":
		case "FLOAT":
			return "0.0";
		case "DECIMAL32":
		case "DECIMAL64":
		case "DECIMAL128":
		case "DECIMAL":
		case "DOUBLE":
			return "0.0";
		case "DATE":
			return "'1970-01-01'";
		case "TIME":
		case "DATETIME":
			return "'1970-01-01 00:00:00'";
		case "TIMESTAMP":
			return "0";
		case "CLOB":
		case "NCLOB":
			return "''";
		case "BLOB":
			return "''";
		case "REAL":
			return "0";
		default:
			return "''";
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

	class SecurityAccess {
		public void disopen() {

		}
	}

	public static void main(String[] args) throws Exception {
		try {
			// Class.forName("com.ultracloud.ultradb.jdbc.UltraDBDriver");
			// Connection con =
			// DriverManager.getConnection("jdbc:ultradb://172.16.11.14:9000/default",
			// "default", "");
			// PreparedStatement stat = con.prepareStatement("SELECT max(CREATE_TIME) FROM
			// T_YJSGTJ");
			//
			// ResultSet rs = stat.executeQuery();
			// if(rs.next()) {
			// System.out.println(rs.getObject(1)+"");
			// }
			// rs.close();
			// stat.close();
			// con.close();
			// String date = "1970-03-01 00:00:00";
			// System.out.println(date.substring(0,10));
			// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			// System.out.println(new java.sql.Date(sdf.parse("1970-01-01
			// 00:00:00").getTime()));
			String type = "create table  analysisplatform_beijing.F_YAYM_TRANSACT_REGISTER_I\n" + 
					"(\n" + 
					"  app_code               String,\n" + 
					"  case_code              String,\n" + 
					"  case_name              String,\n" + 
					"  fillin_organization    String,\n" + 
					"  case_type              String,\n" + 
					"  case_start_date        String,\n" + 
					"  case_end_date          String,\n" + 
					"  report_case_address    String,\n" + 
					"  report_case_date       String,\n" + 
					"  find_form              String,\n" + 
					"  harm_degree            String,\n" + 
					"  occur_section          String,\n" + 
					"  special_case_mark      String,\n" + 
					"  brief_case_info         String,\n" + 
					"  select_occasion        String,\n" + 
					"  select_out_part        String,\n" + 
					"  select_object          String,\n" + 
					"  commit_num             String,\n" + 
					"  commit_tools           String,\n" + 
					"  commit_measure         String,\n" + 
					"  commit_character       String,\n" + 
					"  transact_person        String,\n" + 
					"  case_source            String,\n" + 
					"  transact_idea          String,\n" + 
					"  transact_mode          String,\n" + 
					"  whether_assist         String,\n" + 
					"  whether_search         String,\n" + 
					"  remark                 String,\n" + 
					"  reason                 String,\n" + 
					"  according              String,\n" + 
					"  register_date          String,\n" + 
					"  fillin_person          String,\n" + 
					"  fillin_date            String,\n" + 
					"  handle_name            String,\n" + 
					"  handle_organ           String,\n" + 
					"  accuse_reson           String,\n" + 
					"  transact_place         String,\n" + 
					"  case_state             String,\n" + 
					"  case_class             String,\n" + 
					"  end_case_type          String,\n" + 
					"  case_label             String,\n" + 
					"  law_doc_code           String,\n" + 
					"  solve_case_date        String,\n" + 
					"  case_reason            String,\n" + 
					"  region_flag            String,\n" + 
					"  case_popedom           String,\n" + 
					"  is_urge                String,\n" + 
					"  purse_mode             String,\n" + 
					"  is_in_eight            String,\n" + 
					"  transact_case_code     String,\n" + 
					"  solve_case_flag        String,\n" + 
					"  case_phase             String,\n" + 
					"  secret_level           String,\n" + 
					"  cancel_date            String,\n" + 
					"  cancel_reason          String,\n" + 
					"  solve_type             String,\n" + 
					"  solve_time_sect        String,\n" + 
					"  remove_org             String,\n" + 
					"  remove_man             String,\n" + 
					"  re_man_tel             String,\n" + 
					"  order_adv              String,\n" + 
					"  adv_order              String,\n" + 
					"  adv_date               String,\n" + 
					"  spy_end_adv            String,\n" + 
					"  report_case_type       String,\n" + 
					"  file_ldc               String,\n" + 
					"  change_type_flag       String,\n" + 
					"  change_case_code       String,\n" + 
					"  transact_date          String,\n" + 
					"  adv_dept               String,\n" + 
					"  year                   String,\n" + 
					"  year_num               String,\n" + 
					"  month                  String,\n" + 
					"  month_num              String,\n" + 
					"  case_duty_area         String,\n" + 
					"  lost_money             String,\n" + 
					"  destroy_persons        String,\n" + 
					"  research_results        String,\n" + 
					"  mark_character         String,\n" + 
					"  process_results        String,\n" + 
					"  capture_googs          String,\n" + 
					"  process_date           String,\n" + 
					"  pause                  String,\n" + 
					"  substation             String,\n" + 
					"  popedom_code           String,\n" + 
					"  responsable_no         String,\n" + 
					"  jcj_code               String,\n" + 
					"  diecount               String,\n" + 
					"  harmcount              String,\n" + 
					"  kidnapcount            String,\n" + 
					"  kidnapwomen            String,\n" + 
					"  kidnapchildren         String,\n" + 
					"  totalvalue             String,\n" + 
					"  totallost              String,\n" + 
					"  totalsave              String,\n" + 
					"  casestatus             String,\n" + 
					"  securitylevel          String,\n" + 
					"  ifunitcase             String,\n" + 
					"  ifinoutcase            String,\n" + 
					"  ifabroadcase           String,\n" + 
					"  relationcountry        String,\n" + 
					"  specialcasegrade       String,\n" + 
					"  specialcaseid          String,\n" + 
					"  specialcasetype        String,\n" + 
					"  supervisegrade         String,\n" + 
					"  superviseperson        String,\n" + 
					"  supervisedate          String,\n" + 
					"  shiftinreason          String,\n" + 
					"  shiftinid              String,\n" + 
					"  shiftinstage           String,\n" + 
					"  shiftindate            String,\n" + 
					"  shiftinunit            String,\n" + 
					"  shiftinperson          String,\n" + 
					"  shiftintype            String,\n" + 
					"  shiftincontact         String,\n" + 
					"  shiftinaddress         String,\n" + 
					"  speciality1            String,\n" + 
					"  speciality2            String,\n" + 
					"  speciality3            String,\n" + 
					"  speciality4            String,\n" + 
					"  invademode1            String,\n" + 
					"  invademode2            String,\n" + 
					"  invademode3            String,\n" + 
					"  invademode4            String,\n" + 
					"  fleemode1              String,\n" + 
					"  fleemode2              String,\n" + 
					"  fleemode3              String,\n" + 
					"  fleemode4              String,\n" + 
					"  tools1                 String,\n" + 
					"  tools2                 String,\n" + 
					"  tools3                 String,\n" + 
					"  tools4                 String,\n" + 
					"  fashion1               String,\n" + 
					"  fashion2               String,\n" + 
					"  fashion3               String,\n" + 
					"  fashion4               String,\n" + 
					"  toolssource1           String,\n" + 
					"  toolssource2           String,\n" + 
					"  toolssource3           String,\n" + 
					"  toolssource4           String,\n" + 
					"  commrule1              String,\n" + 
					"  commrule2              String,\n" + 
					"  commrule3              String,\n" + 
					"  commrule4              String,\n" + 
					"  commrule5              String,\n" + 
					"  commrule6              String,\n" + 
					"  zone1                  String,\n" + 
					"  zone2                  String,\n" + 
					"  zone3                  String,\n" + 
					"  zone4                  String,\n" + 
					"  scopeanalyze           String,\n" + 
					"  personcountmax         String,\n" + 
					"  personcountmin         String,\n" + 
					"  ifflow                 String,\n" + 
					"  ifforeigner            String,\n" + 
					"  personzone             String,\n" + 
					"  flowzone               String,\n" + 
					"  processanalyze         String,\n" + 
					"  last_upd               String,\n" + 
					"  main_handle            String,\n" + 
					"  second_handle          String,\n" + 
					"  created                String,\n" + 
					"  join_obj               String,\n" + 
					"  ifrebuild              String,\n" + 
					"  rebuildreason          String,\n" + 
					"  case_time              String,\n" + 
					"  solve_time             String,\n" + 
					"  justice_date           String,\n" + 
					"  sue_date               String,\n" + 
					"  transact_idea_other    String,\n" + 
					"  case_detail_address    String,\n" + 
					"  if_limit_case          String,\n" + 
					"  son_case_type          String,\n" + 
					"  commit_num_new         String,\n" + 
					"  case_start_date_rang   String,\n" + 
					"  case_end_date_rang     String,\n" + 
					"  case_start_date_flag   String,\n" + 
					"  case_end_date_flag     String,\n" + 
					"  commit_num_new_flag    String,\n" + 
					"  case_type_new          String,\n" + 
					"  wastage                String,\n" + 
					"  dead_num               String,\n" + 
					"  injured_num            String,\n" + 
					"  commit_method_describ  String,\n" + 
					"  find_time              String,\n" + 
					"  xckybh                 String,\n" + 
					"  cxsjm                  String,\n" + 
					"  build_case_dw          String,\n" + 
					"  if_sw                  String,\n" + 
					"  swlb                   String,\n" + 
					"  x_zb                   String,\n" + 
					"  y_zb                   String,\n" + 
					"  sssj                   String,\n" + 
					"  ssfxj                  String,\n" + 
					"  sjly_xzqh              String,\n" + 
					"  etl_date               String,\n" + 
					"  rksj                   String,\n" + 
					"  fillin_organization_dic String,\n" + 
					"  case_type_dic          String,\n" + 
					"  report_case_address_dic String,\n" + 
					"  find_form_dic          String,\n" + 
					"  harm_degree_dic        String,\n" + 
					"  occur_section_dic      String,\n" + 
					"  select_occasion_dic    String,\n" + 
					"  select_out_part_dic    String,\n" + 
					"  select_object_dic      String,\n" + 
					"  commit_measure_dic     String,\n" + 
					"  commit_character_dic   String,\n" + 
					"  transact_idea_dic      String,\n" + 
					"  transact_mode_dic      String,\n" + 
					"  whether_assist_dic     String,\n" + 
					"  whether_search_dic     String,\n" + 
					"  handle_organ_dic       String,\n" + 
					"  case_state_dic         String,\n" + 
					"  case_class_dic         String,\n" + 
					"  case_label_dic         String,\n" + 
					"  region_flag_dic        String,\n" + 
					"  case_popedom_dic       String,\n" + 
					"  is_urge_dic            String,\n" + 
					"  purse_mode_dic         String,\n" + 
					"  is_in_eight_dic        String,\n" + 
					"  case_phase_dic         String,\n" + 
					"  secret_level_dic       String,\n" + 
					"  cancel_reason_dic      String,\n" + 
					"  solve_type_dic         String,\n" + 
					"  report_case_type_dic   String,\n" + 
					"  change_type_flag_dic   String,\n" + 
					"  substation_dic         String,\n" + 
					"  securitylevel_dic      String,\n" + 
					"  ifunitcase_dic         String,\n" + 
					"  ifinoutcase_dic        String,\n" + 
					"  ifabroadcase_dic       String,\n" + 
					"  relationcountry_dic    String,\n" + 
					"  specialcasegrade_dic   String,\n" + 
					"  specialcasetype_dic    String,\n" + 
					"  supervisegrade_dic     String,\n" + 
					"  speciality1_dic        String,\n" + 
					"  speciality2_dic        String,\n" + 
					"  speciality3_dic        String,\n" + 
					"  speciality4_dic        String,\n" + 
					"  invademode1_dic        String,\n" + 
					"  invademode2_dic        String,\n" + 
					"  invademode3_dic        String,\n" + 
					"  invademode4_dic        String,\n" + 
					"  fleemode1_dic          String,\n" + 
					"  fleemode2_dic          String,\n" + 
					"  fleemode3_dic          String,\n" + 
					"  fleemode4_dic          String,\n" + 
					"  tools1_dic             String,\n" + 
					"  tools2_dic             String,\n" + 
					"  tools3_dic             String,\n" + 
					"  tools4_dic             String,\n" + 
					"  fashion1_dic           String,\n" + 
					"  fashion2_dic           String,\n" + 
					"  fashion3_dic           String,\n" + 
					"  fashion4_dic           String,\n" + 
					"  toolssource1_dic       String,\n" + 
					"  toolssource2_dic       String,\n" + 
					"  toolssource3_dic       String,\n" + 
					"  toolssource4_dic       String,\n" + 
					"  commrule1_dic          String,\n" + 
					"  commrule2_dic          String,\n" + 
					"  commrule3_dic          String,\n" + 
					"  commrule4_dic          String,\n" + 
					"  commrule5_dic          String,\n" + 
					"  commrule6_dic          String,\n" + 
					"  zone1_dic              String,\n" + 
					"  zone2_dic              String,\n" + 
					"  zone3_dic              String,\n" + 
					"  zone4_dic              String,\n" + 
					"  scopeanalyze_dic       String,\n" + 
					"  ifflow_dic             String,\n" + 
					"  ifforeigner_dic        String,\n" + 
					"  personzone_dic         String,\n" + 
					"  flowzone_dic           String,\n" + 
					"  ifrebuild_dic          String,\n" + 
					"  son_case_type_dic      String,\n" + 
					"  sjly_xzqh_dic          String,\n" + 
					"  rodpssj                String,\n" + 
					"  rjcksj                 String,\n" + 
					"  dwrksj                 String\n" + 
					") engine=MergeTree() partition by substring(rodpssj,1,7) order by rodpssj SETTINGS index_granularity=8192 ;";
			String sql = type;
			String[] engine = sql.toUpperCase().split("ENGINE");
			if (engine.length > 1) {
				if (engine[1].toUpperCase().indexOf("MERGETREE") > -1) {
					
					try {
						ParserMergeTree.parser(sql);
					} catch (Exception e) {

					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AdapterException("连接数据库出错,请检查数据库连接信息，错误详情：" + ex.getMessage());
		}
	}
}
