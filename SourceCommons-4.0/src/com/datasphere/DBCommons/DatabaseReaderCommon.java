package com.datasphere.DBCommons;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.security.Password;
import com.datasphere.source.cdc.common.TableMD;
import com.datasphere.utility.lic.LicManager;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.source.lib.type.dtenum;

public class DatabaseReaderCommon {
	private static Logger logger;
	private String url;
	private String username;
	private String password;
	private List<String> tables;
	private String tablesValue;
	private String checkColumn;

	private String checkColumn2;

	public String getCheckColumn2() {
		return checkColumn2;
	}

	public void setCheckColumn2(String checkColumn2) {
		this.checkColumn2 = checkColumn2;
	}

	private String databaseType;
	private DatabaseHandler dbHandler;
	private Map<String, Integer> wildcardPriority;
	private List<String> excludedTables;
	private Map<String, Object> properties;
	private Map<String, String> startPositionMap;
	private dtenum asDT;
	private int fetchSize;
	private int pollingInterval;
	private Map<String, List<String>> checkColumnMap2;
	private Map<String, String> checkColumnMap;
	private Connection connection;
	private String startPosition;

	public List<String> getTablesList() {
		return this.tables;
	}

	public DatabaseReaderCommon(final Map<String, Object> properties) {
		this.url = null;
		this.username = null;
		this.password = null;
		this.tables = null;
		this.tablesValue = null;
		this.checkColumn = null;
		this.databaseType = null;
		this.wildcardPriority = new HashMap<String, Integer>();
		this.excludedTables = new ArrayList<String>();
		this.startPositionMap = new HashMap<String, String>();
		this.fetchSize = 100;
		this.pollingInterval = 120;
		this.connection = null;
		this.startPosition = null;
		this.properties = properties;
		this.checkColumnMap2 = new HashMap<>();
	}

	public Map<String, Object> getProperties() {
		return this.properties;
	}

	public void setDBHandler() throws Exception {
		try {
			this.dbHandler = new DatabaseHandler(this.properties, this.connection, this.fetchSize);
		} catch (Exception e) {
			throw new Exception(e);
		}
		this.databaseType = this.dbHandler.getDatabaseType();
		this.fetchSize = this.dbHandler.getFetchSize();
	}

	public int getfetchSize() {
		return this.fetchSize;
	}

	public int getpollingInterval() {
		return this.pollingInterval;
	}

	public Connection getConnection() {
		return this.connection;
	}

	public dtenum getasDT() {
		return this.asDT;
	}

	public synchronized void initializeTableList() throws Exception {
		this.tables = new ArrayList<String>();
		try {
			final String[] split = this.tablesValue.split(";");
			for (final String tbl : split) {
				final String tableName = tbl.trim();
				String catalog = null;
				String schema = null;
				String table = null;
				if (tableName.contains(".")) {
					final StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
					if (tokenizer.countTokens() > 3) {
						throw new IllegalArgumentException(
								"Illegal argument in TABLES property found. Expected argument should contain at most 3 dot seperated string. Found '"
										+ tableName + "'");
					}
					if (tokenizer.countTokens() == 3) {
						catalog = tokenizer.nextToken();
					}
					schema = tokenizer.nextToken();
					table = tokenizer.nextToken();
				} else {
					table = tableName;
				}
				if (DatabaseReaderCommon.logger.isDebugEnabled()) {
					DatabaseReaderCommon.logger.debug((Object) ("Trying to fetch table metadata for catalog '" + catalog
							+ "' and schema '" + schema + "' and table pattern '" + table + "'"));
				}
				final DatabaseMetaData md = this.connection.getMetaData();
				if("mysql".equalsIgnoreCase(databaseType)) {
					catalog = schema;
					schema = null;
				}
				final ResultSet tableResultSet = md.getTables(catalog, schema, table,
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
				while (tableResultSet.next()) {
					final String catalogName = tableResultSet.getString(1);
					final String schemaName = tableResultSet.getString(2);
					final String tblName = tableResultSet.getString(3);
					final boolean isExcluded = this.excludedTables.contains(tblName);
					if (!isExcluded) {
						final StringBuilder tableFQN = new StringBuilder();
						if (catalogName != null && !catalogName.trim().isEmpty()
								&& !"cassandra".equalsIgnoreCase(this.databaseType)) {
							tableFQN.append(this.makeUsableSqlName(catalogName) + ".");
						}
						if (schemaName != null && !schemaName.trim().isEmpty()) {
							tableFQN.append(this.makeUsableSqlName(schemaName) + ".");
						}
						tableFQN.append(this.makeUsableSqlName(tblName));
						this.tables.add(tableFQN.toString());
						this.wildcardPriority.put(tableFQN.toString(), 0);
						if (DatabaseReaderCommon.logger.isInfoEnabled()) {
							DatabaseReaderCommon.logger.info((Object) ("Adding table " + tableFQN.toString()
									+ " to the list of tables to be queried"));
						}
					} else if (DatabaseReaderCommon.logger.isInfoEnabled()) {
						DatabaseReaderCommon.logger.info(
								(Object) ("Table " + tblName + " being excluded as it is in explicit ExcludedTables"));
					}
					if (this.tables.isEmpty()) {
						throw new AdapterException("表名不能为空");
					}
				}
			}
		} catch (Exception e) {
			final String errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause()
					+ ";Message : " + e.getMessage();
			final AdapterException exception = new AdapterException(errorString);
			DatabaseReaderCommon.logger.error((Object) errorString);
			throw exception;
		}
	}

	public synchronized void initializeCheckColumnList() throws Exception {
		this.checkColumnMap = new HashMap<String, String>();
		this.checkColumnMap2 = new HashMap<String, List<String>>();
		try {
			final String[] split = this.checkColumn.split(";");
			for (final String tbl : split) {
				final String tableName = tbl.trim().substring(0, tbl.indexOf("="));
				final String columnName = tbl.trim().substring(tbl.indexOf("=") + 1, tbl.length());
				String catalog = null;
				String schema = null;
				String table = null;
				if (tableName.contains(".")) {
					final StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
					if (tokenizer.countTokens() > 3) {
						throw new IllegalArgumentException(
								"Illegal argument in TABLES property found. Expected argument should contain at most 3 dot seperated string. Found '"
										+ tableName + "'");
					}
					if (tokenizer.countTokens() == 3) {
						catalog = tokenizer.nextToken();
					}
					schema = tokenizer.nextToken();
					table = tokenizer.nextToken();
				} else {
					table = tableName;
				}
				if (DatabaseReaderCommon.logger.isDebugEnabled()) {
					DatabaseReaderCommon.logger.debug((Object) ("Trying to fetch table metadata for catalog '" + catalog
							+ "' and schema '" + schema + "' and table pattern '" + table + "'"));
				}
				final DatabaseMetaData md = this.connection.getMetaData();
				if("mysql".equalsIgnoreCase(databaseType)) {
					catalog = schema;
					schema = null;
				}
				final ResultSet tableResultSet = md.getTables(catalog, schema, table,
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
				// <jeq>-this.tables=[public.rdb_incre]
				while (tableResultSet.next()) {
					final String p1 = tableResultSet.getString(1);
					final String p2 = tableResultSet.getString(2);
					final String p3 = tableResultSet.getString(3);
					// [tableResultSet]->p1=null,p2=public,p3=rdb_incre
					// [tableResultSet]->p1=null,p2=public,p3=rdb_incre
					final StringBuilder tableFQN = new StringBuilder();
					if (p1 != null && !p1.trim().isEmpty() && !"cassandra".equalsIgnoreCase(this.databaseType)) {
						tableFQN.append(this.makeUsableSqlName(p1) + ".");
					}
					if (p2 != null && !p2.trim().isEmpty()) {
						tableFQN.append(this.makeUsableSqlName(p2) + ".");
					}
					tableFQN.append(this.makeUsableSqlName(p3));// public.rdb_incre
					final String tmp = tableFQN.toString();
					if (this.tables.contains(tmp)) {
						this.checkColumnMap.put(tmp, columnName);
						List<String> oldArr = this.checkColumnMap2.get(tmp);
						if (oldArr == null || oldArr.size() == 0) {
							List<String> newArr = new ArrayList<>();
							newArr.add(columnName);
							this.checkColumnMap2.put(tmp, newArr);
						} else {
							oldArr.add(columnName);
							this.checkColumnMap2.put(tmp, oldArr);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new AdapterException("初始化检查字段出错：" + e.getMessage());
		}
	}

	public boolean isCorrectCheckMap() {
		for (int i = 0; i < this.tables.size(); ++i) {
			if (!this.checkColumnMap.containsKey(this.tables.get(i))) {
				return false;
			}
		}
		return true;
	}

	public boolean isCorrectStartMap() {
		for (final Map.Entry<String, String> entry : this.startPositionMap.entrySet()) {
			if (!this.tables.contains(entry.getKey())) {
				return false;
			}
		}
		return true;
	}

	public synchronized void initializeStartPositionList() throws Exception {
		try {
			if (this.startPosition == null)
				return;
			final String[] split = this.startPosition.split(";");
			for (final String tbl : split) {
				final String tableName = tbl.trim().substring(0, tbl.indexOf("="));
				String columnName = tbl.trim().substring(tbl.indexOf("=") + 1, tbl.length());
				String catalog = null;
				String schema = null;
				String table = null;
				if (tableName.contains(".")) {
					final StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
					if (tokenizer.countTokens() > 3) {
						throw new IllegalArgumentException(
								"Illegal argument in TABLES property found. Expected argument should contain at most 3 dot seperated string. Found '"
										+ tableName + "'");
					}
					if (tokenizer.countTokens() == 3) {
						catalog = tokenizer.nextToken();
					}
					schema = tokenizer.nextToken();
					table = tokenizer.nextToken();
				} else {
					table = tableName;
				}
				if (DatabaseReaderCommon.logger.isDebugEnabled()) {
					DatabaseReaderCommon.logger.debug((Object) ("Trying to fetch table metadata for catalog '" + catalog
							+ "' and schema '" + schema + "' and table pattern '" + table + "'"));
				}
				final DatabaseMetaData md = this.connection.getMetaData();
				if("mysql".equalsIgnoreCase(databaseType)) {
					catalog = schema;
					schema = null;
				}
				final ResultSet tableResultSet = md.getTables(catalog, schema, table,
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
				boolean validStart = false;
				while (tableResultSet.next()) {
					validStart = true;
					final String p1 = tableResultSet.getString(1);
					final String p2 = tableResultSet.getString(2);
					final String p3 = tableResultSet.getString(3);
					final StringBuilder tableFQN = new StringBuilder();
					if (p1 != null && !p1.trim().isEmpty() && !"cassandra".equalsIgnoreCase(this.databaseType)) {
						tableFQN.append(this.makeUsableSqlName(p1) + ".");
					}
					if (p2 != null && !p2.trim().isEmpty()) {
						tableFQN.append(this.makeUsableSqlName(p2) + ".");
					}
					tableFQN.append(this.makeUsableSqlName(p3));
					final String tmp = tableFQN.toString();
					if (columnName.equals("0")) {
						columnName = "min";
					} else if (columnName.equals("-1")) {
						columnName = "max";
					}
					int wildCardLength;
					if (tableName.indexOf("%") >= 0) {
						wildCardLength = tableName.length() - 1;
					} else {
						wildCardLength = tableName.length();
					}
					if (this.wildcardPriority.containsKey(tmp) && this.wildcardPriority.get(tmp) > wildCardLength) {
						continue;
					}
					if (this.tables.contains(tmp)) {
						this.startPositionMap.put(tmp, columnName);
					}
					this.wildcardPriority.put(tmp, wildCardLength);
				}
				if (!validStart) {
					throw new AdapterException("表 " + tableName + " 不存在");
				}
			}
		} catch (Exception e) {
			throw new AdapterException("初始化开始位置出错：" + e.getMessage());
		}
	}

	// public void disconnect() throws Exception {
	// if (this.connection != null) {
	// this.connection.close();
	// }
	// }

	public void createConnection() throws Exception {
		try {
//			try {
//				LicManager lic = new LicManager();
//				lic.checkLicense("DataExchange 数据共享交换平台");
//			} catch (Exception e1) {
//				e1.printStackTrace();
//				System.exit(0);
//			}
			final Properties connProperties = ConnectionUtil.getConnectionProperties(this.properties);
			this.url = connProperties.getProperty("url");
			if (this.url.startsWith("jdbc:ultradb:")) {
				Class.forName("com.ultracloud.ultradb.jdbc.UltraDBDriver");
			} else if (this.url.startsWith("jdbc:gbase:")) {
				Class.forName("com.gbase.jdbc.Driver");
			} else if (this.url.startsWith("jdbc:postgresql:")) {
				if (this.url.indexOf("stringtype") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?stringtype=unspecified";
					} else {
						this.url += "&stringtype=unspecified";
					}
				}
				if (this.url.indexOf("tcpKeepAlive") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?tcpKeepAlive=true";
					} else {
						this.url += "&tcpKeepAlive=true";
					}
				}
				Class.forName("org.postgresql.Driver");
			} else if (this.url.startsWith("jdbc:dm:")) {
				Class.forName("dm.jdbc.driver.DmDriver");
			} else if (this.url.startsWith("jdbc:informix-sqli:")) {
				Class.forName("com.informix.jdbc.IfxDriver");
			} else if (this.url.startsWith("jdbc:highgo:")) {
				Class.forName("com.highgo.jdbc.Driver");
			} else if (this.url.startsWith("jdbc:mysql:")) {
				if (this.url.indexOf("useCursorFetch=true") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?useCursorFetch=true";
					} else {
						this.url += "&useCursorFetch=true";
					}
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
				if (this.url.indexOf("autoReconnect=") < 0) {
					if (this.url.indexOf("?") < 0) {
						this.url += "?autoReconnect=true";
					} else {
						url += "&autoReconnect=true";
					}
				}
				if (url.indexOf("failOverReadOnly=") < 0) {
					if (url.indexOf("?") < 0) {
						url += "?failOverReadOnly=false";
					} else {
						url += "&failOverReadOnly=false";
					}
				}
				if (url.indexOf("maxReconnects=") < 0) {
					if (url.indexOf("?") < 0) {
						url += "?maxReconnects=1000";
					} else {
						url += "&maxReconnects=1000";
					}
				}
				if (url.indexOf("initialTimeout") < 0) {
					if (url.indexOf("?") < 0) {
						url += "?initialTimeout=30";
					} else {
						url += "&initialTimeout=30";
					}
				}
				try {
					Class.forName("com.mysql.cj.jdbc.Driver");
				} catch (ClassNotFoundException ex) {
					Class.forName("com.mysql.jdbc.Driver");
				}
			} else if (url.startsWith("jdbc:oracle:")) {
				connProperties.put("oracle.net.CONNECT_TIMEOUT", 1000 * 60 * 30);
				connProperties.put("oracle.jdbc.ReadTimeout", 60000 * 60 * 30);
				Class.forName("oracle.jdbc.OracleDriver");
			} else if (this.url.startsWith("jdbc:edb:")) {
				Class.forName("com.edb.Driver");
			} else if (this.url.startsWith("jdbc:teradata:")) {
				Class.forName("com.teradata.jdbc.TeraDriver");
			} else if (this.url.startsWith("jdbc:kingbase:")) {// 人大金仓
				Class.forName("com.kingbase.Driver");
			} else if (this.url.startsWith("jdbc:db2:")) {// DB2
				Class.forName("com.ibm.db2.jdbc.net.DB2Driver");
			} else if (this.url.startsWith("jdbc:t4jdbc:")) {
				Class.forName("org.trafodion.jdbc.t4.T4Driver");
			}
			if (this.url.startsWith("jdbc:mysql:")) {
				this.connection = DriverManager.getConnection(this.url, this.username, this.password);
			} else {
				this.connection = DriverManager.getConnection(this.url, connProperties);
			}
			this.connection.setAutoCommit(false);
			this.connection.setReadOnly(true);
			this.connection.setNetworkTimeout(Executors.newFixedThreadPool(2), 1000 * 60 * 30);
		} catch (SQLException e) {
			final String errorString = " Failure in connecting to Database with url : " + this.url + " username : "
					+ this.username + " \n ErrorCode : " + e.getErrorCode() + ";SQLCode : " + e.getSQLState()
					+ ";SQL Message : " + e.getMessage();
			final Exception exception = new Exception(errorString);
			DatabaseReaderCommon.logger.error((Object) errorString);
			throw exception;
		} catch (Exception e2) {
			e2.printStackTrace();
			final String errorString = " Failure in connecting to Database with url : " + this.url + " username : "
					+ this.username + " \n Cause : " + e2.getCause() + ";Message : " + e2.getMessage();
			final Exception exception = new Exception(errorString);
			DatabaseReaderCommon.logger.error((Object) errorString);
			throw exception;
		}
	}

	public void parseProperties() throws Exception {
		this.url = this.properties.get("ConnectionURL").toString();
		this.username = this.properties.get("Username").toString();
		this.password = ((Password) this.properties.get("Password")).getPlain();
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger
					.trace((Object) ("Initialized DatabaseReaderNew with password: " + this.password));
		}
		if (!this.properties.containsKey("Tables") || this.properties.get("Tables") == null
				|| this.properties.get("Tables").toString().length() <= 0) {
			throw new IllegalArgumentException("Expected required parameter 'Tables' not found");
		}
		this.tablesValue = this.properties.get("Tables").toString();
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger.trace((Object) ("Initialized DatabaseReaderNew with tableList:" + this.tables));
		}
		int fetchS = 5000;
		if (properties.containsKey("FetchSize") && properties.get("FetchSize") != null) {
			Object val = properties.get("FetchSize");
			if (val instanceof Number) {
				fetchS = ((Number) val).intValue();
			} else if (val instanceof String) {
				fetchS = Integer.parseInt((String) val);
			}
			if (fetchS < 0 || fetchS > 1000000000) {
				fetchS = 100;
			}
		}
		this.fetchSize = fetchS;

		this.pollingInterval = 120;
		if (this.properties.containsKey("PollingInterval") && this.properties.get("PollingInterval") != null
				&& this.properties.get("PollingInterval").toString().length() > 0) {
			final String polling = this.properties.get("PollingInterval").toString();
			String pollingVal = new String();
			if (polling.length() > 0 && polling.endsWith("sec")) {
				pollingVal = polling.replace("sec", "").trim();
			} else {
				if (this.properties.get("PollingInterval") instanceof Number) {
					pollingVal = this.properties.get("PollingInterval").toString();
				} else {
					pollingVal = "120";
				}
				DatabaseReaderCommon.logger
						.warn((Object) ("Setting Polling value as default because Polling interval is invalid "
								+ polling + ", format is %dmin, keeping %d as integer, example 5sec"));
			}
			try {
				int interval = Integer.parseInt(pollingVal.trim());
				if (interval < 1) {
					interval = 5;
				}
				this.pollingInterval = interval;
			} catch (NumberFormatException ex) {
				this.pollingInterval = 120;
				DatabaseReaderCommon.logger.warn(
						(Object) ("Setting Polling value as default beacuse Polling interval is invalid " + polling
								+ ", format is %dmin, keeping %d as integer, example 5sec, use integer in range [1-120] and not "
								+ pollingVal));
			}
		}
		if (this.properties.containsKey("CheckColumn") && this.properties.get("CheckColumn") != null
				&& this.properties.get("CheckColumn").toString().length() > 0) {
			this.checkColumn = this.properties.get("CheckColumn").toString().trim();
			if (this.properties.containsKey("StartPosition") && this.properties.get("StartPosition") != null
					&& this.properties.get("StartPosition").toString().length() > 0) {
				this.startPosition = this.properties.get("StartPosition").toString().trim();
			}
			try {
				if (this.properties.containsKey("ExcludedTables") && this.properties.get("ExcludedTables") != null
						&& this.properties.get("ExcludedTables").toString().length() > 0) {
					final String excludedTableStr = this.properties.get("ExcludedTables").toString();
					final String[] split;
					final String[] tabs = split = excludedTableStr.split(";");
					for (final String tab : split) {
						String temp = tab.substring(tab.lastIndexOf(46) + 1);
						if (temp.charAt(0) == '\"' && temp.charAt(temp.length() - 1) == '\"') {
							temp = temp.substring(1, temp.length() - 2);
						}
						this.excludedTables.add(temp);
					}
				}
			} catch (Exception e) {
				final String errMsg = "Invalid value in excludedTables property: " + e.getMessage();
				DatabaseReaderCommon.logger.error((Object) errMsg);
				throw new IllegalArgumentException(errMsg);
			}
			String asDTstr = null;
			if (this.properties.containsKey("ReturnDateTimeAs") && this.properties.get("ReturnDateTimeAs") != null
					&& this.properties.get("ReturnDateTimeAs").toString().trim().length() > 0) {
				asDTstr = this.properties.get("ReturnDateTimeAs").toString().trim();
				if (asDTstr.compareToIgnoreCase("JODA") == 0) {
					this.asDT = dtenum.asJODA;
				} else {
					if (asDTstr.compareToIgnoreCase("String") != 0) {
						final Exception exc = new Exception("Invalid value for ReturnDateTimeAs: " + asDTstr);
						DatabaseReaderCommon.logger.error((Object) exc.getMessage());
						throw exc;
					}
					this.asDT = dtenum.asString;
				}
			} else {
				this.asDT = dtenum.asJODA;
			}
			return;
		}
		final Exception exception = new Exception("CheckColumn specified is not correct");
		DatabaseReaderCommon.logger.error((Object) exception.getMessage());
		throw exception;
	}

	public void parseProperties2() throws Exception {
		if (!this.properties.containsKey("ConnectionURL") || this.properties.get("ConnectionURL") == null
				|| this.properties.get("ConnectionURL").toString().length() <= 0) {
			DatabaseReaderCommon.logger.error((Object) "ConnectionURL is not specified");
			throw new Exception("ConnectionURL is not specified");
		}
		this.url = this.properties.get("ConnectionURL").toString();
		if (!this.properties.containsKey("Username") || this.properties.get("Username") == null
				|| this.properties.get("Username").toString().length() <= 0) {
			DatabaseReaderCommon.logger.error((Object) "Username is not specified");
			throw new Exception("Username is not specified");
		}
		this.username = this.properties.get("Username").toString();
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger
					.trace((Object) ("Initialized DatabaseReaderNew with username: " + this.username));
		}
		if (!this.properties.containsKey("Password") || this.properties.get("Password") == null
				|| ((Password) this.properties.get("Password")).getPlain().length() <= 0) {
			DatabaseReaderCommon.logger.error((Object) "Password is not specified");
			throw new Exception("Password is not specified");
		}
		this.password = ((Password) this.properties.get("Password")).getPlain();
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger
					.trace((Object) ("Initialized DatabaseReaderNew with password: " + this.password));
		}
		if (!this.properties.containsKey("Tables") || this.properties.get("Tables") == null
				|| this.properties.get("Tables").toString().length() <= 0) {
			throw new IllegalArgumentException("Expected required parameter 'Tables' not found");
		}
		this.tablesValue = this.properties.get("Tables").toString();
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger.trace((Object) ("Initialized DatabaseReaderNew with tableList:" + this.tables));
		}
		if (this.properties.containsKey("FetchSize")) {
			if (this.properties.get("FetchSize") == null) {
				this.fetchSize = 100;
				// final Exception exception = new Exception("FetchSize is specified Null");
				// DatabaseReaderCommon.logger.error((Object)exception.getMessage());
				// throw exception;
			}
			final Object val = this.properties.get("FetchSize");
			if (val instanceof Number) {
				this.fetchSize = ((Number) val).intValue();
			} else if (val instanceof String) {
				this.fetchSize = Integer.parseInt((String) val);
			}
			if (this.fetchSize < 0 || this.fetchSize > 1000000000) {
				this.fetchSize = 100;
				// final Exception exception2 = new Exception("FetchSize specified is out of
				// Range");
				// DatabaseReaderCommon.logger.error((Object)exception2.getMessage());
				// throw exception2;
			}
		}
		if (this.properties.containsKey("PollingInterval") && this.properties.get("PollingInterval") != null
				&& this.properties.get("PollingInterval").toString().length() > 0) {
			final String polling = this.properties.get("PollingInterval").toString();
			String pollingVal = new String();
			if (polling.length() > 0 && polling.endsWith("sec")) {
				pollingVal = polling.replace("sec", "").trim();
			} else {
				pollingVal = "120";
				DatabaseReaderCommon.logger
						.warn((Object) ("Setting Polling value as default because Polling interval is invalid "
								+ polling + ", format is %dmin, keeping %d as integer, example 5sec"));
			}
			try {
				int interval = Integer.parseInt(pollingVal.trim());
				if (interval < 1) {
					interval = 5;
				}
				this.pollingInterval = interval;
			} catch (NumberFormatException ex) {
				this.pollingInterval = 120;
				DatabaseReaderCommon.logger.warn(
						(Object) ("Setting Polling value as default beacuse Polling interval is invalid " + polling
								+ ", format is %dmin, keeping %d as integer, example 5sec, use integer in range [1-120] and not "
								+ pollingVal));
			}
		}
		if (this.properties.containsKey("CheckColumn") && this.properties.get("CheckColumn") != null
				&& this.properties.get("CheckColumn").toString().length() > 0) {
			this.checkColumn = this.properties.get("CheckColumn").toString().trim();
		}
		// 放到if外面
		if (this.properties.containsKey("StartPosition") && this.properties.get("StartPosition") != null
				&& this.properties.get("StartPosition").toString().length() > 0) {
			this.startPosition = this.properties.get("StartPosition").toString().trim();
		}
		try {
			if (this.properties.containsKey("ExcludedTables") && this.properties.get("ExcludedTables") != null
					&& this.properties.get("ExcludedTables").toString().length() > 0) {
				final String excludedTableStr = this.properties.get("ExcludedTables").toString();
				final String[] split;
				final String[] tabs = split = excludedTableStr.split(";");
				for (final String tab : split) {
					String temp = tab.substring(tab.lastIndexOf(46) + 1);
					if (temp.charAt(0) == '\"' && temp.charAt(temp.length() - 1) == '\"') {
						temp = temp.substring(1, temp.length() - 2);
					}
					this.excludedTables.add(temp);
				}
			}
		} catch (Exception e) {
			final String errMsg = "Invalid value in excludedTables property: " + e.getMessage();
			DatabaseReaderCommon.logger.error((Object) errMsg);
			throw new IllegalArgumentException(errMsg);
		}
		String asDTstr = null;
		if (this.properties.containsKey("ReturnDateTimeAs") && this.properties.get("ReturnDateTimeAs") != null
				&& this.properties.get("ReturnDateTimeAs").toString().trim().length() > 0) {
			asDTstr = this.properties.get("ReturnDateTimeAs").toString().trim();
			if (asDTstr.compareToIgnoreCase("JODA") == 0) {
				this.asDT = dtenum.asJODA;
			} else {
				if (asDTstr.compareToIgnoreCase("String") != 0) {
					final Exception exc = new Exception("Invalid value for ReturnDateTimeAs: " + asDTstr);
					DatabaseReaderCommon.logger.error((Object) exc.getMessage());
					throw exc;
				}
				this.asDT = dtenum.asString;
			}
		} else {
			this.asDT = dtenum.asJODA;
		}
		return;
	}

	boolean isSqlReserved(final String part) {
		return false;
	}

	boolean needsDelimited(final String part) {
		final char[] charPart = part.toCharArray();
		if (charPart[0] == '_' || Character.isDigit(charPart[0])) {
			return true;
		}
		if (this.isSqlReserved(part)) {
			return true;
		}
		for (final char c : charPart) {
			if (!Character.isLetterOrDigit(c) && c != '_') {
				return true;
			}
		}
		return false;
	}

	private String makeUsableSqlName(final String part) {
		if (this.needsDelimited(part)) {
			return "\"" + part + "\"";
		}
		return part;
	}

	public String getStartPosition(final String tableName) {
		final String startPos = this.startPositionMap.get(tableName);
		if (startPos == null) {
			return "max";
		}
		return startPos;
	}

	public String getMetadataKey() {
		return "TableName";
	}

	public Map<String, TypeDefOrName> getMetadata() throws Exception {
		final HashMap<String, ArrayList<Integer>> tableKeyColumns = new HashMap<String, ArrayList<Integer>>();
		return TableMD.getTablesMetadataFromJDBCConnection(this.tables, this.connection, tableKeyColumns, this.asDT);
	}

	public String getCheckColumn(final String tableName) {
		if (this.checkColumnMap == null)
			return null;
		return this.checkColumnMap.get(tableName);
	}

	public List<String> getCheckColumn2(final String tableName) {
		return this.checkColumnMap2.get(tableName);
	}

	public String getStartPosition() {
		return this.startPosition;
	}

	public void setStartPosition(String startPosition) {
		this.startPosition = startPosition;
	}

	public String getDatabaseType() {
		return this.databaseType.toLowerCase();
	}

	public HDEvent prepareEventFromResultSet(final UUID sourceUUID, final int colCount, final ResultSet results,
			final String tableName) throws SQLException {
		final HDEvent out = new HDEvent(colCount, sourceUUID);
		(out.metadata = new HashMap()).put("TableName", tableName);
		out.metadata.put("OperationName", "SELECT");
		out.metadata.put("ColumnCount", colCount);
		out.data = new Object[colCount];
		StringBuffer columnName = new StringBuffer();
		StringBuffer columnType = new StringBuffer();
		final ResultSetMetaData rsmd = results.getMetaData();
		// for (int i = 0; i < colCount; ++i) {
		// int colIndex = i + 1;
		// int colType = rsmd.getColumnType(colIndex);
		// switch (colType) {
		// case java.sql.Types.LONGNVARCHAR:
		// case java.sql.Types.LONGVARCHAR: {
		// try {
		// InputStream inputStream = results.getBinaryStream(colIndex);
		// ByteArrayOutputStream infoStream = new ByteArrayOutputStream();
		// int len = 0;
		// byte[] bytes = new byte[1024];
		// try {
		// while ((len = inputStream.read(bytes)) != -1) {
		// infoStream.write(bytes, 0, len);
		// }
		// } catch (IOException e1) {
		// throw new Exception("输入流读取异常");
		// } finally {
		// try {
		// inputStream.close(); // 输入流关闭
		// } catch (IOException e) {
		// throw new Exception("输入流关闭异常");
		// }
		// }
		// out.setData(i, infoStream.toString());
		// } catch (Exception e) {
		// System.out.println("第 " + colIndex + " 个字段，类型：" + colType + "");
		// e.printStackTrace();
		// out.setData(i, "");
		// }
		// break;
		// }
		// }
		// }
		for (int i = 0; i < colCount; ++i) {
			int colIndex = i + 1;
			String colName = rsmd.getColumnName(colIndex).toUpperCase();
			int colType = rsmd.getColumnType(colIndex);
			columnName.append(rsmd.getColumnName(colIndex));
			columnType.append(rsmd.getColumnTypeName(colIndex));
			if (i < colCount - 1) {
				columnName.append(",");
				columnType.append(",");
			}
			Object columnValue = null;

			try {
				switch (colType) {
				case java.sql.Types.CHAR:
				case java.sql.Types.NVARCHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.VARCHAR: {
					try {
						columnValue = results.getObject(colIndex);
						if (columnValue != null) {
							columnValue = ((String) columnValue).trim();
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
					columnValue = results.getBoolean(colIndex);
					break;
				}
				case -6: {
					columnValue = results.getShort(colIndex);
					break;
				}
				case 5: {
					columnValue = results.getShort(colIndex);
					break;
				}
				case 4: {
					columnValue = results.getInt(colIndex);
					break;
				}
				case -5: {
					columnValue = results.getLong(colIndex);
					break;
				}
				case 7: {
					columnValue = results.getFloat(colIndex);
					break;
				}
				case 6:
				case 8: {
					columnValue = results.getDouble(colIndex);
					break;
				}
				case java.sql.Types.NUMERIC:
				case java.sql.Types.DECIMAL: {
					columnValue = results.getBigDecimal(colIndex);
					break;
				}
				case 91: {
					java.sql.Date date = results.getDate(colIndex);
					if (date != null) {
						columnValue = LocalDate.fromDateFields((Date) date);
						break;
					}
					break;
				}
				case 93: {
					Timestamp timestamp = results.getTimestamp(colIndex);
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
					Clob clob = results.getClob(colIndex);
					if (clob != null) {
						// int x = 0;
						// try {
						// InputStream input = clob.getAsciiStream();
						// int len = (int) clob.length();
						// byte by[] = new byte[len];
						// while (-1 != (x = input.read(by, 0, by.length))) {
						// input.read(by, 0, x);
						// }
						//
						// value = new String(by, "utf-8");
						// } catch (Exception e) {
						// value = "";
						// }
						// if (value.indexOf("\"") > -1) {
						// System.out.println("发现异常数据结束3：" + value);
						// value = value.replace("\"", "");
						// }
						// columnValue = value;
						columnValue = clob.getSubString((long) 1, (int) clob.length());
					}
					break;
				}
				case 2004:
					try {
						Blob blob = results.getBlob(colIndex);
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
				// case java.sql.Types.LONGNVARCHAR:
				// case java.sql.Types.LONGVARCHAR: {
				// {
				// StringBuffer sb = new StringBuffer("");
				// try {
				// InputStream inputStream = results.getBinaryStream(colIndex);
				// ByteArrayOutputStream infoStream = new ByteArrayOutputStream();
				// int len = 0;
				// byte[] bytes = new byte[1024];
				// try {
				// while ((len = inputStream.read(bytes)) != -1) {
				// infoStream.write(bytes, 0, len);
				// }
				// } catch (IOException e1) {
				// throw new Exception("输入流读取异常");
				// } finally {
				// try {
				// if (inputStream != null)
				// inputStream.close(); // 输入流关闭
				// } catch (IOException e) {
				// throw new Exception("输入流关闭异常");
				// }
				// }
				// if (infoStream != null) {
				// sb.append(infoStream.toString());
				// infoStream.close();
				// }
				// } catch (Exception e) {
				//
				// }
				// // try {
				// // InputStream is = results.getAsciiStream(colIndex);
				// // if (is != null) {
				// // Scanner cin = new Scanner(is);
				// // cin.useDelimiter("\r\n");
				// // while (cin.hasNext()) {
				// // sb.append(cin.next()).append("\n");
				// // }
				// // is.close();
				// // }
				// // } catch (Exception e) {
				// // System.out.println("第 " + colIndex + " 个字段，类型：" + colType + "");
				// // e.printStackTrace();
				// // }
				// columnValue = sb.toString();
				// break;
				// }
				// }
				default: {
					columnValue = results.getObject(colIndex);
					break;
				}

				}
			} catch (Exception e) {
				System.out.println("第 " + colIndex + " 个字段，类型：" + colType + "");
				e.printStackTrace();
			}
			if (results.wasNull()) {
				columnValue = null;
			}
			out.setData(i, columnValue);

		}
		if (DatabaseReaderCommon.logger.isTraceEnabled()) {
			DatabaseReaderCommon.logger.trace((Object) ("Returning event " + out.toString()));
		}
		return out;
	}

	static {
		DatabaseReaderCommon.logger = Logger.getLogger((Class) DatabaseReaderCommon.class);
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
}
