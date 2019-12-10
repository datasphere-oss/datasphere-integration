package com.datasphere.proc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.security.Password;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;


@PropertyTemplate(name = "HiveSecureVerReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "ConnectionString", type = String.class, required = true, defaultValue = "jdbc:hive2://ha-cluster/default;zk.quorum=192.168.0.101,192.168.0.102;zk.port=24002"),
		@PropertyTemplateProperty(name = "osUsername", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "osPassword", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Query", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "500"),
		@PropertyTemplateProperty(name = "IsSecureVer", type = Boolean.class, required = true, defaultValue = "true"),
		@PropertyTemplateProperty(name = "Krb5ConfPath", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "KeytabPath", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "UserPrincipal", type = String.class, required = false, defaultValue = "user.principal=hive/hadoop.hadoop.com;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;zk.principal=zookeeper/hadoop.hadoop.com")

}, inputType = HDEvent.class)
public class HiveSecureVerReader extends SourceProcess {
	private static Logger logger = Logger.getLogger(HiveSecureVerReader.class);
	private String tables = null;
	private String query = null;
	private String dbUser = null;
	private String dbPasswd = null;
	private String dbConnectionString = null;
	private int fetchSize = 500;
	public static String USERNAME = "osUsername";
	public static String PASSWORD = "osPassword";
	public static String QUERY = "Query";
	public static String TABLES = "Tables";
	public static String CONNECTION = "ConnectionString";
	boolean isHDEvent = false;
	private Connection con;
	private boolean isThreadPoolStarted = false;
	private boolean isSecureVer = false;// 所连接的集群是否为安全版本
	private String userPrincipal = "";
	private String krb5ConfPath = "";
	private String keytabPath = "";

	/**
	 * 初始化参数
	 */
	@Override
	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
		try {
			validateProperties(properties);
			isThreadPoolStarted = false;
		} catch (Exception ex) {
			throw new AdapterException("初始化 HiveSecureVerReader 发生错误: " + ex.getMessage());
		}
	}

	/**
	 * 初始化属性
	 * 
	 * @param properties
	 * @throws Exception
	 */
	private void validateProperties(Map<String, Object> properties) throws Exception {
		if ((properties.get(TABLES) != null) && (!properties.get(TABLES).toString().trim().isEmpty())) {
			this.tables = properties.get(TABLES).toString().trim();
		}
		if ((properties.get(QUERY) != null) && (!properties.get(QUERY).toString().trim().isEmpty())) {
			this.query = properties.get(QUERY).toString().trim();
		}
		if ((properties.containsKey(CONNECTION)) && (properties.get(CONNECTION) != null)) {
			this.dbConnectionString = properties.get(CONNECTION).toString();
		}
		if ((properties.get(USERNAME) != null) && (!properties.get(USERNAME).toString().trim().isEmpty())) {
			this.dbUser = properties.get(USERNAME).toString();
		}
		if ((properties.get(PASSWORD) != null) && (!((Password) properties.get(PASSWORD)).getPlain().isEmpty())) {
			this.dbPasswd = ((Password) properties.get(PASSWORD)).getPlain();
		}

		if ((properties.get("IsSecureVer") != null) && ((boolean) properties.get("IsSecureVer"))) {
			this.isSecureVer = true;
		} else {
			this.isSecureVer = false;
		}

		if (isSecureVer) {
			StringBuilder sb = new StringBuilder();
			sb.append(this.dbConnectionString);
			
			if ((properties.get("Krb5ConfPath") != null)
					&& (!properties.get("Krb5ConfPath").toString().trim().isEmpty())) {
				this.krb5ConfPath = properties.get("Krb5ConfPath").toString();
			}
			
			if ((properties.get("KeytabPath") != null) && (!properties.get("KeytabPath").toString().trim().isEmpty())) {
				this.keytabPath = properties.get("KeytabPath").toString();
				sb.append(";user.keytab=").append(this.keytabPath);
			}
			if ((properties.get("UserPrincipal") != null)
					&& (!properties.get("UserPrincipal").toString().trim().isEmpty())) {
				this.userPrincipal = properties.get("UserPrincipal").toString();
				sb.append(";user.principal=").append(this.userPrincipal);
			}
			// Hive Server为HA模式，指定Zookeeper的ip和端口号来查询当前主HiveServer
			// 其中，zkQuorum的"xxx.xxx.xxx.xxx"为集群中Zookeeper所在节点的IP
			// String zkQuorum = "xxx.xxx.xxx.xxx";
			// zkPort为集群中Zookeeper使用的端口号
			// String zkPort = "24002";
			// 拼接JDBC URL

			if (this.krb5ConfPath != null && this.krb5ConfPath.length() > 0) {
				// 设置krb5文件路径
				System.setProperty("java.security.krb5.conf", this.krb5ConfPath);
			}
			// sBuilder.append("jdbc:hive2://ha-cluster/default;zk.quorum=");
			// sBuilder.append(zkQuorum);
			// sBuilder.append(";zk.port=")
			// .append(zkPort));
			// 设置新建用户的userPrincipal，此处填写为带域名的用户名，例如创建的用户为user，其userPrincipal则为user@HADOOP.COM。
			// 如果使用Hive组件的系统启动用户hive/hadoop.hadoop.com，则设置userPrincipal为hive/hadoop.hadoop.com。
			// String userPrincipal = "hive/hadoop.hadoop.com";

			// 设置客户端的keytab文件路径
			// String userKeyTab = "conf/hive.keytab";

			this.dbConnectionString = sb.toString();
		}
		con = getConnect();
	}

	/**
	 * 获取连接
	 * 
	 * @return
	 * @throws Exception
	 */
	public Connection getConnect() throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		return DriverManager.getConnection(this.dbConnectionString, this.dbUser, this.dbPasswd);
	}

	/**
	 * 启动应用
	 */
	@Override
	public void receiveImpl(int arg0, Event arg1) throws Exception {
		if (!this.isThreadPoolStarted) {
			this.isThreadPoolStarted = true;
			query();
		}
	}

	/**
	 * 停止应用
	 */
	@Override
	public void close() throws Exception {
		try {
			if (this.con != null && !this.con.isClosed()) {
				this.con.close();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 查询数据
	 * 
	 * @throws Exception
	 */
	public void query() throws Exception {
		PreparedStatement prestat = null;
		ResultSet rst = null;
		try {
			String sql = null;
			if (this.query == null) {
				sql = "select * from " + this.tables;
			} else {
				sql = this.query;
			}
			prestat = con.prepareStatement(sql);
			prestat.setFetchSize(this.fetchSize);
			rst = prestat.executeQuery();

			int count = rst.getMetaData().getColumnCount();

			while (rst.next()) {
				StringBuffer sb = new StringBuffer();

				HDEvent out = new HDEvent(count, this.sourceUUID);
				out.metadata = new HashMap<>();
				out.metadata.put(Constant.OPERATION, "INSERT");
				out.metadata.put(Constant.TABLE_NAME, this.tables.trim());
				out.typeUUID = this.typeUUID;
				out.data = new Object[count];
				for(int index =0;index < count; index++) {
					sb.append(rst.getMetaData().getColumnName(index + 1));
					if(index < count) {
						sb.append(",");
					}
					out.setData(index, rst.getObject(index + 1));
				}

				out.metadata.put("ColumnName", sb.toString());
				send((Event) out, 0);
			}
			//发送同步完成
			HDEvent out = new HDEvent(count, this.sourceUUID);
			out.metadata = new HashMap<>();
			out.metadata.put(Constant.OPERATION, "INSERT");
			out.metadata.put(Constant.TABLE_NAME, this.tables.trim());
			out.typeUUID = this.typeUUID;
			out.metadata.put("status", "Completed");
			out.data = null;
			send((Event) out, 0);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AdapterException(e.getMessage());
		} finally {
			try {
				if (rst != null) {
					rst.close();
					rst = null;
				}
				if (prestat != null) {
					prestat.close();
					prestat = null;
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new AdapterException(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		HashMap<String, Object> writerProperties = new HashMap<>();
		writerProperties.put("ConnectionString", "jdbc:hive2://117.107.241.79:10000/default");
		writerProperties.put(TABLES, "test");
		writerProperties.put(USERNAME, "root");
		writerProperties.put(PASSWORD, "Y8OWy26mKpAYAx4m");
		try {
			HiveSecureVerReader reader = new HiveSecureVerReader();
			reader.validateProperties(writerProperties);
			reader.query();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
}
