package com.datasphere.proc;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo.Type;
import com.datasphere.security.Password;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "ClickhouseReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = "jdbc:clickhouse://127.0.0.1:9000/default"),
		@PropertyTemplateProperty(name = "Username", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Query", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "1000") }, inputType = HDEvent.class)
public class ClickhouseReader extends SourceProcess {

	private static Logger logger = Logger.getLogger(ClickhouseReader.class);
	private Connection connection = null;
	private Statement statement = null;
	private ResultSet results = null;
	private String url = null;
	private String username = null;
	private String password = null;
	private String tables = null;
	private String query = null;

	private int fetchSize = 1000;
//	private int columnCount = 0;
	private Type dataType = null;
	private List<String[]> columnList = null;
	private List<Integer> defaultValueIndexList = new ArrayList<>();
	private List<String> keyList = new ArrayList<>();
	private List<Integer> keyIndexList = new ArrayList<>();
	private String insertSql = "";
	private String updateSql = "";
	private String deleteSql = "";
	private boolean isMergeTree = false;
	private HashMap<String, Object> metadata = null;
	private String sql = null;
	private UUID sourceUUID = null;

	/**
	 * 初始化
	 */
	@Override
	public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID uuid,
			final String distributionID, final SourcePosition restartPosition, final boolean sendPositions,
			final Flow flow) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
		
		this.sourceUUID = uuid;
		
		if (!properties.containsKey("Username") || properties.get("Username") == null
				|| properties.get("Username").toString().length() <= 0) {
			this.username = "";
		} else {
			this.username = properties.get("Username").toString();
		}
		if (!properties.containsKey("Password") || properties.get("Password") == null
				|| ((Password) properties.get("Password")).getPlain().toString().length() <= 0) {
			this.password = "";
		} else {
			this.password = ((Password) properties.get("Password")).getPlain().toString();
		}

		this.url = properties.get("ConnectionURL").toString();
		if (properties.containsKey("Query") && properties.get("Query") != null) {
			this.query = properties.get("Query").toString();
		}
		if (properties.containsKey("FetchSize") && properties.get("FetchSize") != null) {
//			this.fetchSize =Integer.valueOf(((Long)properties.get("FetchSize")).toString());
//			if (fetchSize < 0 || fetchSize > 1000000000) {
//				final Exception exception2 = new Exception("批量大小超出范围");
//				logger.error((Object) exception2.getMessage());
//				throw exception2;
//			}
		}
		String tbl = properties.get("Tables").toString().trim();

		if (tbl.contains(";")) {
			tbl = tbl.substring(0, tbl.indexOf(";"));
		}
		if (tbl.contains(".") && !tbl.endsWith(".")) {
			this.tables = tbl.substring(tbl.indexOf(".") + 1);
		} else {
			this.tables = tbl;
		}

		if (this.query != null) {
			this.sql = this.query;
		} else {
			this.sql = "select * from " + this.tables;
		}

		Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
		this.connection = DriverManager.getConnection(this.url, this.username, this.password);

		if (this.connection != null) {
			this.statement = this.connection.createStatement();
			this.statement.setFetchSize(fetchSize);
			//final ResultSetMetaData metadata = this.results.getMetaData();
			//this.columnCount = metadata.getColumnCount();
			this.results = this.statement.executeQuery(this.sql);
		}

		this.metadata = new HashMap<String, Object>();
		this.metadata.put("OperationName", "INSERT");
//		this.metadata.put("ColumnCount", this.columnCount);
		this.metadata.put("TableName", this.tables);
		this.metadata.put("SourceType", "Clickhouse");

	}

	/**
	 * 停止任务
	 */
	@Override
	public void close() throws Exception {
		if (this.statement != null && !statement.isClosed()) {
			statement.close();
		}
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	/**
	 * 处理接收到的消息
	 */
	@Override
	public void receiveImpl(int channel, Event event) throws Exception {
		if (this.results != null) {
			while (this.results.next()) {
				int count = this.results.getMetaData().getColumnCount();
				HDEvent out = new HDEvent(count, sourceUUID);
				this.metadata.put("ColumnCount", count);
				out.metadata = this.metadata;
				out.data = new Object[count];
				StringBuffer columnName = new StringBuffer();
				final ResultSetMetaData rsmd = this.results.getMetaData();
				for (int i = 0; i < count; ++i) {
					final int colIndex = i + 1;
					final int colType = rsmd.getColumnType(colIndex);
					columnName.append(rsmd.getColumnName(colIndex));
					if (i < out.data.length - 1) {
						columnName.append(",");
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
					case 7:
					case 100: {
						columnValue = this.results.getFloat(colIndex);
						break;
					}
					case 6:
					case 8:
					case 101: {
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
					case -4: {
						final byte[] bytes = this.results.getBytes(colIndex);
						if (bytes != null) {
							columnValue = DatatypeConverter.printHexBinary(bytes);
							break;
						}
						break;
					}
					case -3:
					case -2: {
						final byte[] bytesbinary = this.results.getBytes(colIndex);
						if (bytesbinary != null) {
							columnValue = bytesbinary;
							break;
						}
						break;
					}
					case 2004: {
						final byte[] bytes2 = this.results.getBytes(colIndex);
						if (bytes2 != null) {
							columnValue = DatatypeConverter.printHexBinary(bytes2);
							break;
						}
						break;
						// ----------以下为旧版本代码
						// try {
						// Blob blob = this.results.getBlob(colIndex);
						// if (blob != null) {
						// InputStream input = blob.getBinaryStream();
						// int len = (int) blob.length();
						// byte b[] = new byte[len];
						// int n = -1;
						// while (-1 != (n = input.read(b, 0, b.length))) {
						// input.read(b, 0, n);
						// }
						// columnValue = b;
						// }
						// } catch (IOException e) {
						// e.printStackTrace();
						// }
						// break;
					}
					case 2005:
						Clob clob = this.results.getClob(colIndex);
						if (clob != null) {
							columnValue = clob.getSubString((long) 1, (int) clob.length());
						}
						break;
					case 93: {
						final Timestamp timestamp = this.results.getTimestamp(colIndex);
						if (timestamp == null) {
							columnValue = null;
							break;
						}
						columnValue = new DateTime((Object) timestamp);
						break;
					}
					default: {
						columnValue = this.results.getString(colIndex);
						break;
					}
					}
					if (this.results.wasNull()) {
						columnValue = null;
					}
					out.setData(i, columnValue);
				}
				out.metadata.put("ColumnName", columnName.toString());

				this.send((Event)out, 0);
			}
		}
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
}
