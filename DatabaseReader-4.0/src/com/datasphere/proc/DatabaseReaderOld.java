package com.datasphere.proc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.metaRepository.HazelcastSingleton;
//import com.datasphere.hikaricp.HikariPoolManager;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.compiler.AST;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.meta.MetaInfo.Source;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.cdc.common.TableMD;
import com.datasphere.source.lib.constant.CDCConstant;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.source.lib.type.dtenum;
import com.datasphere.utility.DBUtils;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.common.AppStatus;
import com.datasphere.proc.entity.EventLog;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.proc.utils.PostgresqlUtils;
import com.datasphere.validate.ValidateField;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;

public class DatabaseReaderOld implements DatabaseReader {
	private static Logger logger;
	private Connection connection;
	private PreparedStatement statement;
	private ResultSet results;
	private int colCount;
	private String query;
	private String typeName;
	private String url;
	private dtenum asDT;
	private String tablesValue = null;
	private String tableName = null;
	private String username = null;
	private String password = null;
	private int preRows = 0;
	private UUID sourceUUID = null;
	private UUID appUUID = null;
	private String[] fields = null;
	private String appName = null;
	private String dbType = null;
	// private boolean isCreateBean = false;// 是否创建实体类
	// private AutoCreateBean autoCreateBean = null;// 创建实体类
	private String createTableSQL = "";
	private String schemaName = null;
	private Source current_stream = null;

	private int currentDataCount = 0;// 当前同步的数据量
	private int totalDataCount = 0;// 总数据量，估计值
	private boolean updating = false;
	private PostgresqlUtils exceptionLogUtils;
	private EventLog eventLog;
	private ScheduledThreadPoolExecutor pool = null;
	private String batchUUID;
	private boolean isSendColumnName = false;

	public DatabaseReaderOld(boolean isSendColumnName) {
		this.connection = null;
		this.statement = null;
		this.results = null;
		this.colCount = 0;
		this.query = null;
		this.typeName = "QUERY";
		this.url = null;
		this.username = null;
		this.password = null;
		this.updating = false;
		this.exceptionLogUtils = new PostgresqlUtils();
		this.eventLog = new EventLog();
		this.isSendColumnName = isSendColumnName;
	}

	@Override
	public synchronized void initDR(final Map<String, Object> properties) throws Exception {
//		System.out.println("初始化 数据库连接组件"+properties.get("sourceUUID"));
		if (properties.get("sourceUUID") != null || properties.get("UUID") != null) {
			this.sourceUUID = (UUID) (properties.get("sourceUUID") == null?properties.get("UUID"):properties.get("sourceUUID"));
			current_stream = (Source) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID,
					WASecurityManager.TOKEN);
			this.appUUID = current_stream.getCurrentApp().getUuid();
			this.appName = current_stream.getCurrentApp().getName();
			
		}
		int fetchSize = 0;
		String asDTstr = null;
		this.url = properties.get("ConnectionURL").toString();
		this.username = properties.get("Username").toString();
		this.password = ((Password) properties.get("Password")).getPlain();

		if (properties.containsKey("ValidateField") && properties.get("ValidateField") != null) {
			this.fields = properties.get("ValidateField").toString().toUpperCase().split(",");
		}
		if ((properties.containsKey("Tables")) && (properties.get("Tables") != null)
				&& (((String) properties.get("Tables")).length() > 0)) {
			this.tablesValue = ((String) properties.get("Tables"));
		}
		// 表名
		if (this.tablesValue.trim().endsWith(";")) {
			tableName = this.tablesValue.trim().substring(0, tablesValue.length() - 1);
		} else {
			tableName = this.tablesValue.trim();
		}
		if (properties.containsKey("Query") && properties.get("Query") != null
				&& properties.get("Query").toString().length() > 0) {
			this.query = properties.get("Query").toString();
			if(this.query.trim().contains("|||")) {
	        		this.query = this.query.replace("|||", "\"");
	        }
		}
		try {
			int fetchS = 1000;
			if (properties.containsKey("FetchSize") && properties.get("FetchSize") != null) {
				Object val = properties.get("FetchSize");
				if (val instanceof Number) {
					fetchS = ((Number) val).intValue();
				} else if (val instanceof String) {
					fetchS = Integer.parseInt((String) val);
				}
				if (fetchS < 0 || fetchS > 100000) {
					fetchS = 1000;
				}
			} else {
				fetchS = 1000;
			}
			fetchSize = fetchS;
		} catch (Exception e3) {
			fetchSize = 1000;
		}
		if (properties.containsKey("ReturnDateTimeAs") && properties.get("ReturnDateTimeAs") != null
				&& properties.get("ReturnDateTimeAs").toString().trim().length() > 0) {
			asDTstr = properties.get("ReturnDateTimeAs").toString().trim();
			if (asDTstr.compareToIgnoreCase("JODA") == 0) {
				this.asDT = dtenum.asJODA;
			} else {
				if (asDTstr.compareToIgnoreCase("String") != 0) {
					Exception exc = new Exception("Invalid value for ReturnDateTimeAs: " + asDTstr);
					logger.error((Object) exc.getMessage());
					throw exc;
				}
				this.asDT = dtenum.asString;
			}
		} else {
			this.asDT = dtenum.asJODA;
		}
		if (logger.isDebugEnabled()) {
			logger.debug((Object) ("Retrieving from url '" + this.url + "'"));
		}
		if (this.url.startsWith("jdbc:postgresql:")) {
			// if(this.url.indexOf("stringtype=unspecified") < 0) {
			// if(this.url.indexOf("?") < 0) {
			// this.url += "?stringtype=unspecified";
			// }else {
			// this.url += "stringtype=unspecified";
			// }
			// }
		} else if (this.url.startsWith("jdbc:mysql:")) {
			if (this.url.indexOf("useCursorFetch=") < 0) {
				if (this.url.indexOf("?") < 0) {
					this.url += "?useCursorFetch=true";
				} else {
					this.url += "&useCursorFetch=true";
				}
				if (this.url.indexOf("useSSL=") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?useSSL=false";
					} else {
						this.url += "&useSSL=false";
					}
				}
				if (this.url.indexOf("serverTimezone=") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?serverTimezone=GMT";
					} else {
						this.url += "&serverTimezone=GMT";
					}
				}
			}
		}
		// try {
		// this.connection = DriverManager.getConnection(this.url, this.username,
		// this.password);
		// this.connection.setAutoCommit(false);
		// } catch (Exception e) {
		try {
			if (this.url.startsWith("jdbc:ultradb:")) {
				Class.forName("com.ultracloud.ultradb.jdbc.UltraDBDriver");
			} else if (this.url.startsWith("jdbc:gbase:")) {
				Class.forName("com.gbase.jdbc.Driver");
			} else if (this.url.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (this.url.startsWith("jdbc:dm:")) {
				Class.forName("dm.jdbc.driver.DmDriver");
			} else if (this.url.startsWith("jdbc:informix-sqli:")) {
				Class.forName("com.informix.jdbc.IfxDriver");
			} else if (this.url.startsWith("jdbc:highgo:")) {
				Class.forName("com.highgo.jdbc.Driver");
			} else if (this.url.startsWith("jdbc:mysql:")) {
				try {
					Class.forName("com.mysql.cj.jdbc.Driver");
				} catch (ClassNotFoundException ex) {
					Class.forName("com.mysql.jdbc.Driver");
				}
			} else if (this.url.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.OracleDriver");
			} else if (this.url.startsWith("jdbc:kingbase:")) {// 人大金仓
				Class.forName("com.kingbase.Driver");
			} else if (this.url.startsWith("jdbc:db2:")) {// DB2
				Class.forName("com.ibm.db2.jdbc.net.DB2Driver");
			} else if (this.url.startsWith("jdbc:edb:")) {
				Class.forName("com.edb.Driver");
			} else if (this.url.startsWith("jdbc:teradata:")) {
				Class.forName("com.teradata.jdbc.TeraDriver");
			} else if (this.url.startsWith("jdbc:t4jdbc:")) {
				Class.forName("org.trafodion.jdbc.t4.T4Driver");
			}
			long start = System.currentTimeMillis();
			this.connection = DriverManager.getConnection(this.url, this.username, this.password);
			// try {
			// this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			// }catch(Exception e) {
			//
			// }
			long end = System.currentTimeMillis();
			System.out.println("----连接耗时：" + ((end - start) / 1000) + "秒");
			this.connection.setAutoCommit(false);
			this.connection.setReadOnly(true);
			this.connection.setNetworkTimeout(Executors.newFixedThreadPool(2), 1000 * 60 * 30);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			String errorString = "连接数据库失败: " + this.url + " 用户名: " + this.username + " \n 原因: " + ex.getCause()
					+ ";消息: " + ex.getMessage();
			exceptionLogUtils.addException(current_stream.getCurrentApp().getUuid().getUUIDString(),
					current_stream.getCurrentApp().getName(), HazelcastSingleton.getBindingInterface(),
					new Timestamp(System.currentTimeMillis()), "AdapterException", "连接数据库出错：" + ex.getMessage(),
					eventLog);
			logger.error((Object) errorString);
			throw new AdapterException(errorString);
		}catch(Exception e) {
			e.printStackTrace();
			String errorString = "连接数据库失败: " + this.url + " 用户名: " + this.username + " \n 原因: " + e.getCause()
			+ ";消息: " + e.getMessage();
			exceptionLogUtils.addException(current_stream.getCurrentApp().getUuid().getUUIDString(),
					current_stream.getCurrentApp().getName(), HazelcastSingleton.getBindingInterface(),
					new Timestamp(System.currentTimeMillis()), "AdapterException", "连接数据库出错：" + e.getMessage(),
					eventLog);
			throw new AdapterException(errorString);
		}
		// }
		String tblName = this.tableName;
		if (this.tableName.indexOf(".") > -1) {
			String[] tbls = this.tableName.split("\\.");
			if (tbls.length == 3) {
				this.schemaName = tbls[1];
				tblName = tbls[2];
			} else if (tbls.length == 2) {
				this.schemaName = tbls[0];
				tblName = tbls[1];
			}
		}
		
		if (properties.containsKey("Query") && properties.get("Query") != null
				&& properties.get("Query").toString().length() > 0) {
			this.query = properties.get("Query").toString();
			// this.results = this.statement.executeQuery(this.query);
		} else {
			this.query = "SELECT * FROM " + this.tableName;
			// this.results = this.statement.executeQuery(this.query);
		}
		dbType = DBUtils.getDbTypeByUrl(this.url);
		if ("postgresql".equalsIgnoreCase(dbType) || "mysql".equalsIgnoreCase(dbType)) {
			if (fetchSize > 1000) {
				fetchSize = 1000;
			}
			this.statement = this.connection.prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.FETCH_FORWARD);
		} else {
			this.statement = this.connection.prepareStatement(this.query);
		}
		if (this.schemaName != null && this.schemaName.length() > 0) {
			try {
				if (!this.url.startsWith("jdbc:teradata:")) {
					this.connection.setSchema(this.schemaName);
				}
			} catch (Exception e) {

			}
		}
		this.statement.setFetchSize(fetchSize);
		long start = System.currentTimeMillis();
		this.results = this.statement.executeQuery();
		long end = System.currentTimeMillis();
		if (((end - start) / 1000) > 30) {
			System.out.println("----正在调整查询速率：原查询耗时：" + ((end - start) / 1000) + "秒");
			int newFetchSize = fetchSize / 2;
			if(this.results.getMetaData() != null) {
				if(this.results.getMetaData().getColumnCount()>300) {
					newFetchSize = newFetchSize > 2? 2 : newFetchSize;
				}else if(this.results.getMetaData().getColumnCount()>200) {
					newFetchSize = newFetchSize > 10? 10 : newFetchSize;
				}else if(this.results.getMetaData().getColumnCount()>100) {
					newFetchSize = newFetchSize > 100? 100 : newFetchSize;
				}
			}
			this.results.close();
			start = System.currentTimeMillis();
			this.statement.setFetchSize(newFetchSize);
			this.results = this.statement.executeQuery();
			end = System.currentTimeMillis();
		}
		System.out.println("----查询" + tblName + "数据量耗时：" + ((end - start) / 1000) + "秒");
		ResultSetMetaData metadata = this.results.getMetaData();
		this.colCount = metadata.getColumnCount();
		if ("oracle".equalsIgnoreCase(dbType)) {
			String sql = "SELECT NUM_ROWS FROM USER_TABLES WHERE TABLE_NAME='" + tblName + "'";
			try (PreparedStatement stat = this.connection.prepareStatement(sql)) {
				ResultSet rs = stat.executeQuery();
				if (rs.next()) {
					totalDataCount = rs.getObject(1) == null ? 0 : rs.getInt(1);
					System.out.println("----预估总数据量：" + totalDataCount);
				}
				if (totalDataCount == 0) {
					// if (rs != null) {
					// rs.close();
					// }
					// sql = "select count(1) from " + tblName;
					//
					// PreparedStatement stat2 = this.connection.prepareStatement(sql);
					// rs = stat2.executeQuery();
					// if (rs.next()) {
					// totalDataCount = rs.getInt(1);
					// System.out.println("----总实际数据量：" + totalDataCount);
					// }
					// if (stat2 != null) {
					// stat2.close();
					// }
				}
				if (rs != null) {
					rs.close();
				}
			}
		} else if ("postgresql".equalsIgnoreCase(dbType)) {

			String sql = "select  reltuples as rowCounts from pg_class where relkind = 'r' and relnamespace = (select oid from pg_namespace where nspname='"
					+ this.schemaName + "') and relname = '" + tblName + "' ";
			try (PreparedStatement stat = this.connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				stat.setFetchDirection(ResultSet.FETCH_FORWARD);
				ResultSet rs = stat.executeQuery();
				if (rs.next()) {
					totalDataCount = rs.getInt(1);
					System.out.println("----预估总数据量：" + totalDataCount);
				}
				if (totalDataCount == 0) {
					// if (rs != null) {
					// rs.close();
					// }
					// sql = "select count(1) from " + tblName;
					// PreparedStatement stat2 = this.connection.prepareStatement(sql);
					// rs = stat2.executeQuery();
					// if (rs.next()) {
					// totalDataCount = rs.getInt(1);
					// System.out.println("----实际总数据量：" + totalDataCount);
					// }
					// if (stat2 != null) {
					// stat2.close();
					// }
				}

				if (rs != null) {
					rs.close();
				}
			}
		} else if ("mysql".equalsIgnoreCase(dbType)) {
			String sql = "SELECT TABLE_ROWS FROM information_schema.tables  WHERE TABLE_NAME='" + tblName
					+ "' and  table_schema = '" + this.schemaName + "'";
			try (PreparedStatement stat = this.connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				ResultSet rs = stat.executeQuery();
				if (rs.next()) {
					totalDataCount = rs.getInt(1);
					System.out.println("----预估总数据量：" + totalDataCount);
				}
				if (totalDataCount == 0) {
					// if (rs != null) {
					// rs.close();
					// }
					// sql = "select count(1) from " + tblName;
					// PreparedStatement stat2 = this.connection.prepareStatement(sql);
					// rs = stat2.executeQuery();
					// if (rs.next()) {
					// totalDataCount = rs.getInt(1);
					// System.out.println("----实际总数据量：" + totalDataCount);
					// }
					// if (stat2 != null) {
					// stat2.close();
					// }
				}
				if (rs != null) {
					rs.close();
				}
			}
		} else {
			// String sql = "select count(1) from " + tblName;
			// try(Statement stat = this.connection.createStatement()){
			// ResultSet rs = stat.executeQuery(sql);
			// if(rs.next()) {
			// totalDataCount = rs.getInt(1);
			// System.out.println("----实际总数据量："+ totalDataCount);
			// }
			// if(rs!=null) {
			// rs.close();
			// }
			// }
			totalDataCount = 0;
		}

		// if(this.createTableSQL.length() == 0) {
		// createTableDDL(this.schemaName,tblName);
		// }
		// 判断是否要生成实体类
		// autoCreateBean = new AutoCreateBean();
		// this.isCreateBean = autoCreateBean.getIsCreateBean(appName);

		if (this.appUUID != null) {
			batchUUID = getMD5(System.currentTimeMillis() + "");
			eventLog();
			eventLog.setBatch_uuid(batchUUID);
			eventLog.setApp_uuid(this.appUUID.getUUIDString());
			eventLog.setApp_name(this.appName);
			eventLog.setCluster_name(HazelcastSingleton.getClusterName());
			eventLog.setNode_id(HazelcastSingleton.getBindingInterface());
			currentDataCount = 0;
			eventLog.setApp_progress("0%");
		}
		updating = true;
	}

	private void eventLog() {
		try {
			pool = new ScheduledThreadPoolExecutor(1);
			pool.scheduleAtFixedRate(() -> {
				if (updating && eventLog.getEvent_starttime() != null) {
					updating = false;
					if (this.appUUID != null) {
						exceptionLogUtils.insertOrUpdate(eventLog); /** 记录数据Event的日志 和 数据交换日志 */
					}
					updating = true;
				}
			}, 1000, 5 * 1000, TimeUnit.MILLISECONDS);
		} catch (Exception e) {

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

	/**
	 * 生成实体类
	 */
	private void createBean(String tableName, ResultSet rs) {
		try {
			File file = new File(
					System.getProperty("user.dir") + File.separator + "src" + File.separator + tableName + ".class");
			if (file.exists()) {
				// System.out.println("----实体类已存在："+tableName);
				return;
			} else {
				// ResultSetMetaData metadata = rs.getMetaData();
				// autoCreateBean.create(tableName, metadata);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public HDEvent nextEvent(final UUID sourceUUID) throws NoSuchElementException, SQLException {
		if (this.results.next()) {
			if (this.appUUID != null) {
				if (eventLog.getEvent_starttime() == null) {
					eventLog.setEvent_starttime(new Timestamp(System.currentTimeMillis()));
				}
				eventLog.setApp_status(AppStatus.APP_READING);
				eventLog.setRead_total(++currentDataCount);// 读总量
				if (totalDataCount > 0) {
					float progress = ((float) currentDataCount / (float) totalDataCount) * 100;
					if (progress <= 100) {
						eventLog.setApp_progress(String.format("%.2f", progress) + "%");
					} else {
						eventLog.setApp_progress("100%");
					}
				} else {
					eventLog.setApp_progress("100%");
				}
			}
			// if(updating && eventLog.getEvent_starttime() != null) {
			// updating = false;
			// exceptionLogUtils.insertOrUpdate(eventLog);
			// updating = true;
			// }

			HDEvent out = new HDEvent(this.colCount, sourceUUID);
			out.metadata = new HashMap<>();
			out.metadata.put("OperationName", "SELECT");
			out.metadata.put("TableName", this.tableName);
			out.metadata.put("ColumnCount", this.colCount);
			out.metadata.put("SourceType", this.dbType);
			out.metadata.put("BatchUUID", this.batchUUID);
			StringBuffer columnName = new StringBuffer();
			StringBuffer columnType = new StringBuffer();
			boolean validated = true;

			out.data = new Object[this.colCount];
			ResultSetMetaData rsmd = this.results.getMetaData();
//			for (int i = 0; i < this.colCount; ++i) {
//				int colIndex = i + 1;
//				int colType = rsmd.getColumnType(colIndex);
//				switch (colType) {
//				case java.sql.Types.LONGNVARCHAR:
//				case java.sql.Types.LONGVARCHAR: {
//					try {
//						InputStream inputStream = results.getBinaryStream(colIndex);
//						ByteArrayOutputStream infoStream = new ByteArrayOutputStream();
//						int len = 0;
//						byte[] bytes = new byte[1024];
//						try {
//							while ((len = inputStream.read(bytes)) != -1) {
//								infoStream.write(bytes, 0, len);
//							}
//						} catch (IOException e1) {
//							throw new Exception("输入流读取异常");
//						} finally {
//							try {
//								inputStream.close(); // 输入流关闭
//							} catch (IOException e) {
//								throw new Exception("输入流关闭异常");
//							}
//						}
//						out.setData(i, infoStream.toString());
//					} catch (Exception e) {
//						System.out.println("第 " + colIndex + " 个字段，类型：" + colType + "");
//						e.printStackTrace();
//						out.setData(i, "");
//					}
//					break;
//				}
//				}
//			}
			for (int i = 0; i < this.colCount; ++i) {
				int colIndex = i + 1;
				String colName = rsmd.getColumnName(colIndex).toUpperCase();
				int colType = rsmd.getColumnType(colIndex);
				columnName.append(rsmd.getColumnName(colIndex));
				columnType.append(rsmd.getColumnTypeName(colIndex));
				if (i < this.colCount - 1) {
					columnName.append(",");
					columnType.append(",");
				}
				boolean isskip = false;
				Object columnValue = null;
				switch (colType) {
				case java.sql.Types.CHAR:
				case java.sql.Types.NVARCHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.VARCHAR: {
					try {
						columnValue = results.getObject(colIndex);
						if(columnValue != null) {
							columnValue = ((String)columnValue).trim();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					// if(value != null) {
					// value = value.replaceAll("[^\\u0000-\\uFFFF]", "");
					// }
					// if (value != null && value.indexOf("\"") > -1) {
					// System.out.println("发现异常数据结束1：" + value);
					// value = value.replace("\"", "");
					// }
					break;
				}

				case -7: {
					columnValue = this.results.getBoolean(colIndex);
					break;
				}
				case -6: {
					columnValue = this.results.getShort(colIndex);
					break;
				}
				case 5: {
					columnValue = this.results.getShort(colIndex);
					break;
				}
				case 4: {
					columnValue = this.results.getInt(colIndex);
					break;
				}
				case -5: {
					columnValue = this.results.getLong(colIndex);
					break;
				}
				case 7: {
					columnValue = this.results.getFloat(colIndex);
					break;
				}
				case 6:
				case 8: {
					columnValue = this.results.getDouble(colIndex);
					break;
				}
				case java.sql.Types.NUMERIC:
				case java.sql.Types.DECIMAL: {
					columnValue = this.results.getBigDecimal(colIndex);
					break;
				}
				case 91: {
					java.sql.Date date = this.results.getDate(colIndex);
					if (date != null) {
						columnValue = LocalDate.fromDateFields((Date) date);
						break;
					}
					break;
				}
				case 93: {
					Timestamp timestamp = this.results.getTimestamp(colIndex);
					if (timestamp == null) {
						columnValue = null;
						break;
					}
					switch (this.asDT) {
					case asJODA: {
						long millis = timestamp.getTime();
						int nanos = timestamp.getNanos();
						if (nanos % 1000000 >= 500000) {
							++millis;
						}
						columnValue = new DateTime(millis);
						break;
					}
					case asString: {
						columnValue = timestamp.toString();
						break;
					}
					default: {
						columnValue = new DateTime((Object) timestamp);
						break;
					}
					}
					break;
				}
				case java.sql.Types.CLOB:
				case java.sql.Types.NCLOB:
				case java.sql.Types.LONGNVARCHAR:
				case java.sql.Types.LONGVARCHAR: {
					String value = "";
					Clob clob = this.results.getClob(colIndex);
					if(clob != null) {
						columnValue = clob.getSubString((long) 1, (int) clob.length());
					}
//					if (clob != null) {
//						int x = 0;
//						try {
//							InputStream input = clob.getAsciiStream();
//							int len = (int) clob.length();
//							byte by[] = new byte[len];
//							while (-1 != (x = input.read(by, 0, by.length))) {
//								input.read(by, 0, x);
//							}
//
//							value = new String(by, "utf-8");
//						} catch (Exception e) {
//							value = "";
//						}
//						if (value.indexOf("\"") > -1) {
//							System.out.println("发现异常数据结束3：" + value);
//							value = value.replace("\"", "");
//						}
//						columnValue = value;
//						// columnValue = clob.getSubString((long) 1, (int) clob.length());
//					}
					break;
				}
				case 2004:
					try {
						Blob blob = this.results.getBlob(colIndex);
						if (blob != null) {
							InputStream input = blob.getBinaryStream();
							int len = (int) blob.length();
							byte b[] = new byte[len];
							int n = -1;
							while (-1 != (n = input.read(b, 0, b.length))) {
								input.read(b, 0, n);
							}
							columnValue = b;
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					break;
//				case java.sql.Types.LONGNVARCHAR:
//				case java.sql.Types.LONGVARCHAR: {
//					StringBuffer sb = new StringBuffer("");
//					try {
////						InputStream is = results.getAsciiStream(colIndex);
////						if(is!=null) {
////				            Scanner cin = new Scanner(is);
////				            cin.useDelimiter("\r\n");
////				            while(cin.hasNext()){
////				                sb.append(cin.next()).append("\n");
////				            }
////				            is.close();
////						}
//						InputStream inputStream = results.getBinaryStream(colIndex);
//						ByteArrayOutputStream infoStream = new ByteArrayOutputStream();
//						int len = 0;
//						byte[] bytes = new byte[1024];
//						try {
//							while ((len = inputStream.read(bytes)) != -1) {
//								infoStream.write(bytes, 0, len);
//							}
//						} catch (IOException e1) {
//							throw new Exception("输入流读取异常");
//						} finally {
//							try {
//								if(inputStream!=null)inputStream.close(); // 输入流关闭
//							} catch (IOException e) {
//								throw new Exception("输入流关闭异常");
//							}
//						}
//						if(infoStream != null) {
//							sb.append(infoStream.toString());
//							infoStream.close();
//						}
//					} catch (Exception e) {
//						System.out.println("第 " + colIndex + " 个字段，类型：" + colType + "");
//						e.printStackTrace();
//					}
//					columnValue = sb.toString();
//					break;
//				}
				default: {

					columnValue = this.results.getObject(colIndex);
					break;
				}
				}
				if (this.results.wasNull()) {
					columnValue = null;
				}
				if (!isskip) {
					out.setData(i, columnValue);
				}
				try {
					if (fields != null && fields.length > 0) {
						if (ArrayUtils.contains(fields, colName)) {
							if (!ValidateField.validate(columnValue)) {
								validated = false;
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(isSendColumnName) {
				out.metadata.put("ColumnName", columnName);
				out.metadata.put("ColumnType", columnType);
			}
			// out.metadata.put("CreateTableSQL", this.createTableSQL);
			if (!validated) {
				String msg = out.toJSON();
				out = null;
				ValidateField.appendWriteLog(this.appName, msg);
			}
			// System.out.println("----"+out.toJSON());
			return out;
			// }
		}

		// while(!updating) {
		// try {
		// Thread.sleep(1000);
		// } catch (InterruptedException e) {
		// }
		// }
		updating = false;
		if (this.appUUID != null) {
			eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
			eventLog.setEvent_total(currentDataCount);// 数据交换总量
			eventLog.setApp_status(AppStatus.APP_FINISHED);
			eventLog.setApp_progress("100%");
			exceptionLogUtils.insertOrUpdate(eventLog); /** 记录数据Event的日志 和 数据交换日志 */
		}
		throw new NoSuchElementException("同步完成");
	}

	public static String getUTFStringByEncoding(String str) {
		String encode = "UTF-8";
		String returnStr = "";
		try {
			if (str != null) {
				if (str.equals(new String(str.getBytes("GB2312"), "GB2312"))) {
					encode = "GB2312";
				} else if (str.equals(new String(str.getBytes("ISO-8859-1"), "ISO-8859-1"))) {
					encode = "ISO-8859-1";
				} else if (str.equals(new String(str.getBytes("UTF-8"), "UTF-8"))) {
					encode = "UTF-8";
				} else if (str.equals(new String(str.getBytes("GBK"), "GBK"))) {
					encode = "GBK";
				}
				if (encode.equals("UTF-8")) {
					returnStr = str;
				} else {
					returnStr = new String(str.getBytes(encode), "UTF-8");
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return returnStr;
	}

	@Override
	public synchronized void close() throws SQLException {
		try {
			if (this.appUUID != null && this.appUUID.getUUIDString() != null && eventLog != null
					&& eventLog.getEvent_starttime() != null) {
				eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
				eventLog.setEvent_total(currentDataCount);// 数据交换总量
				eventLog.setApp_status(AppStatus.APP_FINISHED);
				eventLog.setApp_progress("100%");
				exceptionLogUtils.insertOrUpdate(eventLog);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (this.results == null) {
			logger.info((Object) "ResultSet is null");
		} else {
			this.results.close();
		}
		if (this.statement == null) {
			logger.info((Object) "PreparedStatement is null ");
		} else {
			this.statement.close();
		}
		int count = 0;
		// while(!this.updating) {
		// try {
		// count++;
		// if(count>60) {
		// System.out.println("等待源端事件记录任务完成...");
		// break;
		// }
		// Thread.sleep(500);
		// }catch (Exception e) {
		// }
		// }
		// logger.info("等待源端事件记录任务结束");
		// System.out.println("OK");
		if (this.pool != null) {
			this.pool.shutdown();
			this.pool.purge();
			pool = null;
		}
		if (this.connection == null) {
			logger.info((Object) "Connection is null");
		} else {
			this.connection.close();
			if (logger.isDebugEnabled()) {
				logger.debug((Object) "Closed database connection sucessfully");
			}
		}
		ValidateField.close();
		this.exceptionLogUtils.close();
	}

	public Map<String, TypeDefOrName> getMetadata() throws Exception {
		Map<String, TypeDefOrName> tableMetadata = new HashMap<String, TypeDefOrName>();
		DatabaseColumn column = null;
		try {
			DatabaseMetaData dmd = this.connection.getMetaData();

			column = DatabaseColumn.initializeDataBaseColumnFromProductName(dmd.getDatabaseProductName());
		} catch (Exception e2) {
			logger.error((Object) "Unable to identify the DatabaseColumn class for this database.");
		}
		try {
			ResultSetMetaData metadata = this.results.getMetaData();
			int colCount = metadata.getColumnCount();
			List<TypeField> columnList = new ArrayList<TypeField>(colCount);
			List<String> columnNameList = new ArrayList<String>(colCount);
			for (int i = 1; i <= colCount; ++i) {
				String columnName = metadata.getColumnName(i);
				if (columnName == null || columnName.isEmpty()) {
					columnName = "Column_" + i;
					logger.warn((Object) ("Column name with index " + i
							+ " is empty. Corresponding field name for this column index is " + columnName));
				} else {
					columnNameList.add(columnName);
				}
			}
			columnNameList = (List<String>) TableMD.updateDuplicateColumns(columnNameList);
			for (int i = 1; i <= colCount; ++i) {
				TypeName tn = null;
				if (column != null) {
					column.setInternalColumnType(metadata.getColumnTypeName(i));
					tn = AST.CreateType(CDCConstant
							.getCorrespondingClassForCDCType(column.getInternalColumnType().getType(), this.asDT), 0);
				}
				columnList.add(AST.TypeField((String) columnNameList.get(i - 1),
						(tn != null) ? tn : AST.CreateType("string", 0), false));
			}
			TypeDefOrName tableDef = new TypeDefOrName(this.typeName, columnList);
			tableMetadata.put(this.typeName, tableDef);
		} catch (SQLException e) {
			logger.error((Object) ("Failed to create DataExchange type. " + e));
			exceptionLogUtils.addException(current_stream.getCurrentApp().getUuid().getUUIDString(),
					current_stream.getCurrentApp().getName(), HazelcastSingleton.getBindingInterface(),
					new Timestamp(System.currentTimeMillis()), "AdapterException", "获取元数据信息错误", eventLog);

			throw e;
		}
		return tableMetadata;
	}

	public String getMetadataKey() {
		return "TableName";
	}

	@Override
	public String getCurrentTableName() {
		return this.typeName;
	}

	/**
	 * 获取 SQL 语句中的表名
	 * 
	 * @param sql
	 * @param dbtype
	 * @return
	 */
	private static String getTableNames(String sql, EDbVendor dbtype) {
		TGSqlParser sqlParser = new TGSqlParser(dbtype);
		sqlParser.sqltext = sql;
		sqlParser.parse();

		TStatementList stList = sqlParser.sqlstatements;
		if (stList.size() > 0) {
			TCustomSqlStatement customSt = stList.get(0);
			return customSt.tables.getElement(0).toString();
		}
		return null;
	}

	/**
	 * 将 SQL 语句中的表名加双引号
	 * 
	 * @param sql
	 * @return
	 */
	public static String replaceTableName(String sql) {
		String tableName = getTableNames(sql, EDbVendor.dbvoracle);
		if (tableName != null) {
			return replaceTableName(sql, tableName);
		}
		return null;
	}

	/**
	 * 将 SQL 语句中的表名加双引号
	 * 
	 * @param sql
	 * @param tableName
	 * @return
	 */
	public static String replaceTableName(String sql, String tableName) {
		String newTableName = "";
		if (tableName.contains(".")) {
			StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
			if (tokenizer.countTokens() == 2) {
				newTableName = tokenizer.nextToken();
				newTableName += ".";
				newTableName += "\"" + tokenizer.nextToken() + "\"";
			} else {
				newTableName = "\"" + tokenizer.nextToken() + "\"";
			}
		} else {
			newTableName = "\"" + tableName + "\"";
		}
		return sql.replace(tableName, newTableName);
	}

	static {
		logger = Logger.getLogger(DatabaseReaderOld.class);
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		String value = "00:00:00";
		if (value.indexOf("\"") > -1) {
			System.out.println("发现异常数据结束1：" + value);
		}
		if (value.indexOf("#0") > -1) {
			System.out.println("发现异常数据结束2：" + value);
			if (value.endsWith("#0")) {
				value.replace("#0", "");
			}
		}
		System.out.println(value);
		// String sql = "select * from public.Test where id=2";
		// System.out.println(replaceTableName(sql,"public.Test"));
		// System.out.println(replaceTableName(sql,getTableNames(sql,EDbVendor.dbvoracle)));
		// String tbl = AutoCreateBean.upperCamel("test");
		// Class.forName("com.mysql.jdbc.Driver");
		// Connection connection =
		// DriverManager.getConnection("jdbc:mysql://117.107.241.79:3306/buffer_test",
		// "root",
		// "gg!007");
		//
		// connection.setAutoCommit(false);
		// Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
		// ResultSet.CONCUR_READ_ONLY);
		// statement.setFetchSize(1000);
		// ResultSet results = statement.executeQuery("select * from person");
		// while (results.next()) {
		// System.out.println(results.getString(1));
		// }
		// try {
		// File file = new File(
		// System.getProperty("user.dir") + File.separator + "src" + File.separator +
		// tbl + ".class");
		// if (file.exists()) {
		// // System.out.println("----实体类已存在："+tableName);
		// return;
		// } else {
		// ResultSetMetaData metadata = results.getMetaData();
		// AutoCreateBean autoCreateBean = new AutoCreateBean();
		// autoCreateBean.create(tbl, metadata);
		// }
		// } catch (Exception ex) {
		// ex.printStackTrace();
		// }
		// results.close();
		// statement.close();
		// connection.close();
		// MyClassLoader.findClassByNameAndLocation(tbl, "",
		// System.getProperty("user.dir") + File.separator + "src" + File.separator);
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
