package com.datasphere.proc;

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
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.datasphere.common.constants.Constant;
//import com.datasphere.hikaricp.HikariPoolManager;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.meta.MetaInfo.Source;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.cdc.common.TableMD;
import com.datasphere.source.lib.type.dtenum;
import com.datasphere.utility.DBUtils;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.validate.ValidateField;

public class DatabaseReaderNew implements DatabaseReader {
	private Connection connection;
	private Statement statement;
	private ResultSet results;
	private List<String> tables;
	private List<String> excludedTables;
	private int currentTableIndex;
	private String currentTableName;
	private int colCount;
	private String tablesValue;
	private int fetchSize;
	private String url;
	private dtenum asDT;
	private String username = null;
	private String password = null;
	private static Logger logger;
//	private int preRows = 0;
	private UUID sourceUUID = null;
	private UUID appUUID = null;
	private String dbType = null;
	private String validateField = null;
	private String appName = null;
//	private HikariPoolManager pool = null;
	private int parallel = 2;//并行度，只有 Oracle 适用
	
	public DatabaseReaderNew() {
		this.connection = null;
		this.statement = null;
		this.results = null;
		this.tables = null;
		this.excludedTables = new ArrayList<String>();
		this.currentTableIndex = 0;
		this.currentTableName = null;
		this.colCount = 0;
		this.tablesValue = null;
		this.fetchSize = 100;
		this.url = null;
		this.username = null;
		this.password = null;
	}

	@Override
	public synchronized void initDR(final Map<String, Object> properties) throws Exception {
		String asDTstr = null;
		try {
			if (!properties.containsKey("ConnectionURL") || properties.get("ConnectionURL") == null
					|| properties.get("ConnectionURL").toString().length() <= 0) {
				logger.error((Object) "ConnectionURL is not specified");
				throw new Exception("ConnectionURL is not specified");
			}
			this.url = properties.get("ConnectionURL").toString().trim();
		} catch (ClassCastException e3) {
			logger.error((Object) "Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
			throw new Exception("Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
		}
		try {
			if (!properties.containsKey("Username") || properties.get("Username") == null
					|| properties.get("Username").toString().length() <= 0) {
				logger.error((Object) "Username is not specified");
				throw new Exception("Username is not specified");
			}
			this.username = properties.get("Username").toString();
		} catch (ClassCastException e3) {
			logger.error((Object) "Invalid Username format.Value specified cannot be cast to java.lang.String");
			throw new Exception("Invalid Username format.Value specified cannot be cast to java.lang.String");
		}
		
		try {
			if (!properties.containsKey("Password") || properties.get("Password") == null
					|| ((Password) properties.get("Password")).getPlain().length() <= 0) {
				logger.error((Object) "Password is not specified");
				throw new Exception("Password is not specified");
			}
			this.password = ((Password) properties.get("Password")).getPlain();
		} catch (ClassCastException e3) {
			logger.error((Object) "Invalid Password format.Value specified cannot be cast to java.lang.String");
			throw new Exception("Invalid Password format.Value specified cannot be cast to java.lang.String");
		}
		
		this.validateField = properties.get("ValidateField") == null ? null : properties.get("ValidateField").toString();
		try {
			if (properties.containsKey("Tables") && properties.get("Tables") != null
					&& properties.get("Tables").toString().length() > 0) {
				this.tablesValue = properties.get("Tables").toString();
			} else {
				if (!properties.containsKey("Table") || properties.get("Table") == null
						|| properties.get("Table").toString().length() <= 0) {
					throw new IllegalArgumentException("Expected required parameter 'Tables' not found");
				}
				this.tablesValue = properties.get("Table").toString();
			}
		} catch (ClassCastException e3) {
			logger.error((Object) "Invalid Table format.Value specified cannot be cast to java.lang.String");
			throw new Exception("Invalid Table format.Value specified cannot be cast to java.lang.String");
		}
		try {
			if (properties.containsKey("ExcludedTables") && properties.get("ExcludedTables") != null
					&& properties.get("ExcludedTables").toString().length() > 0) {
				String excludedTableStr = properties.get("ExcludedTables").toString();
				String[] split = excludedTableStr.split(";");
				for (String tab : split) {
					String temp = tab.substring(tab.lastIndexOf(46) + 1);
					if (temp.charAt(0) == '\"' && temp.charAt(temp.length() - 1) == '\"') {
						temp = temp.substring(1, temp.length() - 2);
					}
					this.excludedTables.add(temp);
				}
			}
		} catch (Exception e) {
			String errMsg = "Invalid value in excludedTables property: " + e.getMessage();
			logger.error((Object) errMsg);
			throw new IllegalArgumentException(errMsg);
		}
		try {
			int fetchS = 100;
			if (properties.containsKey("FetchSize")) {
				if (properties.get("FetchSize") == null) {
					Exception exception = new Exception("FetchSize is specified Null");
					logger.error((Object) exception.getMessage());
					throw exception;
				}
				Object val = properties.get("FetchSize");
				if (val instanceof Number) {
					fetchS = ((Number) val).intValue();
				} else if (val instanceof String) {
					fetchS = Integer.parseInt((String) val);
				}
				if (fetchS < 0 || fetchS > 1000000000) {
					Exception exception2 = new Exception("FetchSize specified is out of Range");
					logger.error((Object) exception2.getMessage());
					throw exception2;
				}
			} else {
				fetchS = 100;
			}
			this.fetchSize = fetchS;
		} catch (ClassCastException e3) {
			Exception exception = new Exception(
					"Invalid FetchSize format. Value specified cannot be cast to java.lang.Integer");
			logger.error((Object) exception.getMessage());
			throw exception;
		} catch (NumberFormatException e4) {
			Exception exception = new Exception(
					"Invalid FetchSize format. Value specified cannot be cast to java.lang.Integer");
			logger.error((Object) exception.getMessage());
			throw exception;
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
		try {
//			pool =new HikariPoolManager();
//			pool.dataSourceConfig(this.url,this.username,this.password);
//			this.connection = pool.getConnection();
			if (this.url.startsWith("jdbc:ultradb:")) {
				Class.forName("com.ultracloud.ultradb.jdbc.UltraDBDriver");
			}else if (this.url.startsWith("jdbc:gbase:")) {
				Class.forName("com.gbase.jdbc.Driver");
			}else if (this.url.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
				if(this.url.indexOf("stringtype=unspecified") < 0) {
	        			if(this.url.indexOf("?") < 0) {
	        				this.url += "?stringtype=unspecified";
	        			}else {
	        				this.url += "stringtype=unspecified";
	        			}
	        		}
			}else if (this.url.startsWith("jdbc:dm:")) {
				Class.forName("dm.jdbc.driver.DmDriver");
			}else if(this.url.startsWith("jdbc:informix-sqli:")) {
				Class.forName("com.informix.jdbc.IfxDriver");
			}else if(this.url.startsWith("jdbc:highgo:")) {
				Class.forName("com.highgo.jdbc.Driver");
			}else if(this.url.startsWith("jdbc:mysql:")) {
				if(this.url.indexOf("useCursorFetch=true") < 0) {
					if(this.url.indexOf("?")<0) {
						this.url += "?useCursorFetch=true";
					}else {
						this.url += "&useCursorFetch=true";
					}
				}
				Class.forName("com.mysql.jdbc.Driver");
			}else if(this.url.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.OracleDriver");
			}else if(this.url.startsWith("jdbc:kingbase:")) {//人大金仓
				Class.forName("com.kingbase.Driver");
			}else if(this.url.startsWith("jdbc:db2:")) {//DB2
				Class.forName("com.ibm.db2.jdbc.net.DB2Driver");
			}
			this.connection = DriverManager.getConnection(this.url, this.username, this.password);
			this.connection.setAutoCommit(false);
		} catch (SQLException e2) {
			String errorString = " Failure in connecting to Database with url : " + this.url + " username : " + username
					+ " \n ErrorCode : " + e2.getErrorCode() + ";SQLCode : " + e2.getSQLState() + ";SQL Message : "
					+ e2.getMessage();
			Exception exception2 = new Exception(errorString);
			logger.error((Object) errorString);
			throw exception2;
		} catch (Exception e) {
			String errorString = " Failure in connecting to Database with url : " + this.url + " username : " + username
					+ " \n Cause : " + e.getCause() + ";Message : " + e.getMessage();
			Exception exception2 = new Exception(errorString);
			logger.error((Object) errorString);
			throw exception2;
		}
		if (properties.get("sourceUUID") != null) {
			this.sourceUUID = (UUID) properties.get("sourceUUID");
			Source current_stream = (Source) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID,
					WASecurityManager.TOKEN);
			this.appUUID = current_stream.getCurrentApp().getUuid();
			this.appName = current_stream.getCurrentApp().getName();
//			this.preRows = getPosition(this.appUUID.getUUIDString());
//			logger.info("----上次同步位置：" + this.preRows);
		}
		if (properties.containsKey("Parallel") && properties.get("Parallel") != null) {
			this.parallel = Integer.parseInt(properties.get("Parallel").toString());
			System.out.println("读取并行度：" + this.parallel);
		}
		String databaseType = DBUtils.getDbTypeByUrl(this.url);
		
		if("postgresql".equalsIgnoreCase(databaseType) || "greenplum".equalsIgnoreCase(databaseType)) {
			this.statement = this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			try {
				this.statement.execute("SET max_parallel_workers_per_gather TO " + this.parallel);
			}catch(Exception e) {
				System.out.println("当前数据库不支持并行查询："+e.getMessage());
			}
		}else {
			this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
		this.initializeTableList();
		if (!properties.containsKey("SessionType")) {
			this.initializeNextResultSet();
		}
	}

	boolean needsDelimited(final String part) {
		char[] charPart = part.toCharArray();
		if (charPart[0] == '_' || Character.isDigit(charPart[0])) {
			return true;
		}
		if (this.isSqlReserved(part)) {
			return true;
		}
		for (char c : charPart) {
			if (!Character.isLetterOrDigit(c) && c != '_') {
				return true;
			}
		}
		return false;
	}

	boolean isSqlReserved(final String part) {
		return false;
	}

	private String makeUsableSqlName(final String part) {
		if (this.needsDelimited(part)) {
			return "\"" + part + "\"";
		}
		return part;
	}

	private synchronized void initializeTableList() throws Exception {
		this.tables = new ArrayList<String>();
		try {
			String[] split;
			String[] tablesArray = split = this.tablesValue.split(";");
			for (String tbl : split) {
				String tableName = tbl.trim();
				String catalog = null;
				String schema = null;
				String table = null;
				if (tableName.contains(".")) {
					StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
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
				if (logger.isDebugEnabled()) {
					logger.debug((Object) ("Trying to fetch table metadata for catalog '" + catalog + "' and schema '"
							+ schema + "' and table pattern '" + table + "'"));
				}
				DatabaseMetaData md = this.connection.getMetaData();
				ResultSet tableResultSet = md.getTables(catalog, schema, table,
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
				while (tableResultSet.next()) {
					String p1 = tableResultSet.getString(1);
					String p2 = tableResultSet.getString(2);
					String p3 = tableResultSet.getString(3);
					boolean isExcluded = this.excludedTables.contains(p3);
					if (!isExcluded) {
						StringBuilder tableFQN = new StringBuilder();
						if (p1 != null && !p1.trim().isEmpty()) {
							tableFQN.append(this.makeUsableSqlName(p1) + ".");
						}
						if (p2 != null && !p2.trim().isEmpty()) {
							tableFQN.append(this.makeUsableSqlName(p2) + ".");
						}
						tableFQN.append(this.makeUsableSqlName(p3));
						this.tables.add(tableFQN.toString());
						if (!logger.isInfoEnabled()) {
							continue;
						}
						logger.info((Object) ("Adding table " + tableFQN.toString()
								+ " to the list of tables to be queried"));
					} else {
						if (!logger.isInfoEnabled()) {
							continue;
						}
						logger.info((Object) ("Table " + p3 + " being excluded as it is in explicit ExcludedTables"));
					}
				}
				if (this.tables.size() == 0) {
					this.tables.add(schema + ".\"" + table + "\"");
				}
			}
		} catch (Exception e) {
			String errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause()
					+ ";Message : " + e.getMessage();
			Exception exception = new Exception(errorString);
			logger.error((Object) errorString);
			throw exception;
		}
	}

	private synchronized void initializeNextResultSet() throws SQLException, NoSuchElementException {
		try {
			if (this.results != null) {
				this.results.close();
				this.results = null;
				if (this.statement != null) {
					this.statement.close();
					this.statement = null;
				}
			}
			if (this.currentTableIndex >= this.tables.size()) {
				throw new NoSuchElementException("No more tables to be read");
			}
			this.currentTableName = this.tables.get(this.currentTableIndex++);
			if(this.currentTableName.endsWith(";")) {
				this.currentTableName = this.currentTableName.substring(0, this.currentTableName.length()-1);
			}
			if (logger.isDebugEnabled()) {
				logger.debug((Object) ("Going to read from Table " + this.currentTableName));
			}
			String query = null;
			
			if(dbType != null && Constant.ORACLE_TYPE.equalsIgnoreCase(dbType)) {
				query = "SELECT /*+parallel("+this.currentTableName+",4)*/ * FROM " + this.currentTableName;
			}else {
				query = "SELECT * FROM " + this.currentTableName;
			}
			
			//String query = "Select * from " + this.currentTableName;
			this.connection.setAutoCommit(false);
			String dbType = DBUtils.getDbTypeByUrl(this.url);
			if("postgresql".equalsIgnoreCase(dbType)) {
				this.statement = this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			}else {
				this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			}
			this.statement.setFetchSize(this.fetchSize);
			this.results = this.statement.executeQuery(query);
			ResultSetMetaData metadata = this.results.getMetaData();
			this.colCount = metadata.getColumnCount();
		} catch (IndexOutOfBoundsException ioe) {
			throw new NoSuchElementException("No more tables to read from the tableList");
		}
	}

	@Override
	public HDEvent nextEvent(final UUID sourceUUID) throws NoSuchElementException, SQLException {
		if (this.results != null && this.results.next()) {
			if (logger.isTraceEnabled()) {
				logger.trace((Object) "Preparing event from an open Resultset");
			}
			return this.prepareEventFromResultSet(this.results, sourceUUID);
		}
		this.initializeNextResultSet();
		if (this.results != null && this.results.next()) {
			return this.prepareEventFromResultSet(this.results, sourceUUID);
		}
		if (logger.isTraceEnabled()) {
			logger.trace((Object) "Returning null event");
		}
		return null;
	}

	private HDEvent prepareEventFromResultSet(final ResultSet rs, final UUID sourceUUID) throws SQLException {
		final HDEvent out = new HDEvent(this.colCount, sourceUUID);
		out.metadata = new HashMap<>();
		out.metadata.put("TableName", this.currentTableName.toUpperCase());
		out.metadata.put("OperationName", "SELECT");
		out.metadata.put("ColumnCount", this.colCount);
		
		String columnName = "";
		boolean validated = true;
		
		String[] fields = null;
		if(this.validateField != null) {
			fields = this.validateField.toUpperCase().split(",");
		}
		out.data = new Object[this.colCount];
		final ResultSetMetaData rsmd = this.results.getMetaData();
		for (int i = 0; i < this.colCount; ++i) {
			final int colIndex = i + 1;
			String colName = rsmd.getColumnName(colIndex).toUpperCase();
			final int colType = rsmd.getColumnType(colIndex);
			columnName += rsmd.getColumnName(colIndex);
			if(i<this.colCount-1) {
				columnName += ",";
			}
			Object columnValue = null;
			switch (colType) {
				case -1:
				case 1:
				case 12: {
					columnValue = this.results.getString(colIndex);
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
				case 2:
				case 3: {
					columnValue = this.results.getString(colIndex);
					break;
				}
				case 91: {
					final java.sql.Date date = this.results.getDate(colIndex);
					if (date != null) {
						columnValue = LocalDate.fromDateFields((Date) date);
						break;
					}
					break;
				}
				case -102:
				case -101:
				case 93: {
					final Timestamp timestamp = this.results.getTimestamp(colIndex);
					if (timestamp == null) {
						columnValue = null;
						break;
					}
					switch (this.asDT) {
					case asJODA: {
						long millis = timestamp.getTime();
						final int nanos = timestamp.getNanos();
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
				case 2005:
					Clob clob = this.results.getClob(colIndex);
					if (clob != null) {
						columnValue = clob.getSubString((long) 1, (int) clob.length());
					}
					break;
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
				default: {
					columnValue = this.results.getString(colIndex);
					break;
				}
			}
			try {
				if (this.results.wasNull()) {
					columnValue = null;
				}
			} catch (Exception e) {
				columnValue = null;
			}
			out.setData(i, columnValue);
			try {
				if(fields != null && fields.length > 0) {
					if(ArrayUtils.contains(fields,colName)) {
						if (!ValidateField.validate(columnValue)) {
							validated = false;
						}
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		out.metadata.put("ColumnName", columnName);
		if(!validated) {
			ValidateField.appendWriteLog(this.appName, out.toJSON());
			return null;
		}
		
		return out;
	}

	@Override
	public synchronized void close() throws SQLException {
		if (this.results == null) {
			logger.info((Object) "ResultSet is null");
		} else {
			this.results.close();
		}
		if (this.statement == null) {
			logger.info((Object) "PreparedStatement is null, ");
		} else {
			this.statement.close();
		}
//		if(pool != null) {
//			try {
//				pool.freeConnection(this.connection);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
		if (this.connection == null) {
			logger.info((Object) "Connection is null");
		} else {
			this.connection.close();
			if (logger.isDebugEnabled()) {
				logger.debug((Object) "Closed database connection sucessfully");
			}
		}
	}

	public String getMetadataKey() {
		return "TableName";
	}

	public Map<String, TypeDefOrName> getMetadata() throws Exception {
		HashMap<String, ArrayList<Integer>> tableKeyColumns = new HashMap<String, ArrayList<Integer>>();
		return (Map<String, TypeDefOrName>) TableMD.getTablesMetadataFromJDBCConnection((List) this.tables,
				this.connection, (HashMap) tableKeyColumns, this.asDT);
	}

	@Override
	public String getCurrentTableName() {
		return this.currentTableName.replace('.', '_');
	}

	/**
	 * 将 SQL 语句中的表名加双引号
	 * @param sql
	 * @param tableName
	 * @return
	 */
	public static String replaceTableName(String sql,String tableName) {
		String newTableName = "";
		if (tableName.contains(".")) {
			StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
			if (tokenizer.countTokens() == 2) {
				newTableName = tokenizer.nextToken();
				newTableName += ".";
				newTableName += "\"" + tokenizer.nextToken() + "\"";
			}else {
				newTableName = "\"" + tokenizer.nextToken() + "\"";
			}
		}else {
			newTableName = "\"" + tableName + "\"";
		}
		return sql.replace(tableName, newTableName);
	}
	
	static {
		logger = Logger.getLogger(DatabaseReaderNew.class);
	}
	
	public static void main(String[] args) {
		System.out.println(replaceTableName("public.Test","public.Test"));
	}
	
	class SecurityAccess {
		public void disopen() {
			
		}
    }
}
