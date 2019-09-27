package com.datasphere.proc;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.UnknownHostException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class HBaseAuthUtil {

//	private final static Logger logger = Logger.getLogger(HBaseAuthUtil.class);
	private static Configuration config = null;
	private static Connection hbaseConnection;

	private static String confPath = null;
	private static ScheduledThreadPoolExecutor timepool = null;

	public static void init(String mConfPath) {
		if (mConfPath == null) {
//			logger.info("conf目录为null,默认 user.dir");
			mConfPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
		}
		if (!mConfPath.endsWith(File.separator)) {
			mConfPath += File.separator;
		}
		confPath = mConfPath;
		System.out.println("----HBase配置文件目录："+confPath);
	}

	/**
	 * 得到 Hbase 连接
	 * 
	 * @return
	 */
	public static synchronized Connection getConnectionHbase() {
		Configuration configInstance = getConfigInstance();
		if (configInstance != null) {
			if (hbaseConnection == null) {
				try {
					hbaseConnection = ConnectionFactory.createConnection(config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return hbaseConnection;
		}
		return null;
	}

	/**
	 * 定时登录
	 */
	public static void timerLogin() {
		try {
			closeTimer();
			timepool = new ScheduledThreadPoolExecutor(1);
			timepool.scheduleAtFixedRate(() -> {
				try {
					UserGroupInformation.getLoginUser().reloginFromKeytab();
				} catch (IOException e) {
					// e.printStackTrace();
				}
				System.out.println("krb5,重新登陆");
			}, 10, 10 * 3600 * 1000, TimeUnit.MILLISECONDS);
		} catch (Exception e) {

		}
	}

	/**
	 * 关闭定时登录
	 */
	public static void closeTimer() {
		try {
			if (timepool != null) {
				timepool.shutdown();
				timepool.purge();
			}
		} catch (Exception e) {

		}
		System.out.println("准备安全退出认证...");
	}

	/**
	 * 得到配置实例
	 * 
	 * @return
	 */
	public static synchronized Configuration getConfigInstance() {
		if (config == null) {
			config = getConfiguration();
		}
		return config;
	}

	/**
	 * 加载 hbase 配置文件 load hbase configuration files and auth files use configuration
	 * files create hbase configuration instance use autfiles longin hbase and
	 * zookeeper
	 * 
	 * @return Configuration
	 * @throws IOException
	 */
	public static Configuration getConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		
		File f = new File(confPath);
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			if (files != null) {
				for (File file : files) {
					if (file.getName().toLowerCase().endsWith(".xml")) {
						configuration.addResource(new Path(file.getAbsolutePath()));
					}
				}
			}
		} else {
			configuration.addResource(new Path(confPath));
		}
		
//		 configuration.set("hbase.master", "115.29.37.214:6000");
//		 configuration.set("hbase.zookeeper.quorum","115.29.37.214");
//		 configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
		if (User.isHBaseSecurityEnabled(configuration) || User.isSecurityEnabled()) {
			System.setProperty("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");
			System.setProperty("java.security.auth.login.config", confPath + "jaas.conf");
			System.setProperty("java.security.krb5.conf", confPath + "krb5.conf");
			configuration.set("username.client.keytab.file", confPath + "user.keytab");
			configuration.set("username.client.kerberos.principal", "dinfo");
			boolean loginFlag = login(configuration);
			if (loginFlag) {
				System.out.println("Login HDFS Successful!");
//				logger.info("Login HDFS Successful!");
			} else {
				System.out.println("Login failed.");
//				logger.error("Login failed.");
			}
			try {
				User.login(configuration, "username.client.keytab.file", "username.client.kerberos.principal",
						InetAddress.getLocalHost().toString());
				ZKUtil.loginClient(configuration, "username.client.keytab.file", "username.client.kerberos.principal",
						InetAddress.getLocalHost().toString());
			} catch (UnknownHostException e) {
//				logger.error("Error cannot resovle the host: " + e.getMessage(), e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IO 异常：" + e.getMessage());
				e.printStackTrace();
//				logger.error("Error login faild: " + e.getMessage(), e);
			}
		}

		return configuration;
	}

	/**
	 * 得到 hbase 连接 use hbase configuration get hbase Connection instance from
	 * ConnectionFactory hbase and zookeeper have to longin success
	 * 
	 * @author c_sulinbing
	 * @return Connection
	 */
	public static Connection getConnection() {
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
		} catch (IOException e) {
//			logger.error("Error to get connection: " + e.getMessage(), e);
			e.printStackTrace();
		}
		return connection;
	}

	/**
	 * 登录验证 use the hadoop configuration login to hdfs
	 * 
	 * @param configuration
	 * @return
	 */
	public static Boolean login(Configuration configuration) {
		boolean flag = false;
		UserGroupInformation.setConfiguration(configuration);
		try {
			UserGroupInformation.loginUserFromKeytab(configuration.get("username.client.kerberos.principal"),
					configuration.get("username.client.keytab.file"));
			// System.out.println("UserGroupInformation.isLoginKeytabBased():" +
			// UserGroupInformation.isLoginKeytabBased());
			flag = true;
		} catch (IOException e) {
//			logger.error("Login failed: " + e.getMessage(), e);
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 得到 hbase Admin 对象
	 * 
	 * @return HBase Admin Object
	 */
	public static Admin getAdmin() {
		Admin admin = null;
		try {
			admin = getConnection().getAdmin();
		} catch (IOException e) {
//			logger.error("Error to get admin instance: " + e.getMessage(), e);
			e.printStackTrace();
		}
		return admin;
	}

	/**
	 * 得到表
	 * 
	 * @param tableName
	 * @return
	 */
	public static Table getTable(String tableName) {
		Table table = null;
		try {
			table = getConnection().getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
//			logger.error("Error to get table instance: " + e.getMessage(), e);
			e.printStackTrace();
		}
		return table;
	}

	public static void main(String[] args) {
		// hadoop 主目录
		 System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + File.separator + "lib");
		if (args.length == 0) {
			HBaseAuthUtil.init("/usr/local/hbase");
		}else {
			HBaseAuthUtil.init(args[0]);
		}
		try {
			HBaseAdmin admin = new HBaseAdmin(getConfigInstance());
			HTableDescriptor[] tableDescriptor = admin.listTables();
			for (int i = 0; i < tableDescriptor.length; i++) {
				for(HColumnDescriptor des : tableDescriptor[i].getFamilies()) {
					des.getNameAsString();
				}
				System.out.println("------------------------------"+tableDescriptor[i].getNameAsString());
			}
			Connection connection = HBaseAuthUtil.getConnection();
			
			TableName tname = TableName.valueOf("target_test");
			Table table = connection.getTable(tname);
			Scan scan = new Scan();
			scan.addFamily("fi".getBytes());
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				List<Cell> listCells = result.listCells();
				for (Cell cell : listCells) {
					System.out.println("------rowkey" + Bytes.toString(CellUtil.cloneRow(cell)));
					System.out.println("------family" + Bytes.toString(CellUtil.cloneFamily(cell)));
					System.out.println("------qualifier" + Bytes.toString(CellUtil.cloneQualifier(cell)));
					System.out.println("------data" + Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			scanner.close();
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
