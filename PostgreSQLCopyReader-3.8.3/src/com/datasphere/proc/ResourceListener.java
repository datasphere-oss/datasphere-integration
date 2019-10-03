package com.datasphere.proc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class ResourceListener implements FileAlterationListener {

//	ZJPFileMonitor monitor = null;
	private static Connection connection = null;
	private static String tableName = null;
	private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

	/**
	 * 初始化，并将现有文件入库
	 * @param dbConnectionString
	 * @param dbUser
	 * @param dbPasswd
	 * @param mTableName
	 * @param filePath
	 */
	public ResourceListener(String dbConnectionString, String dbUser, String dbPasswd, String mTableName,String filePath) {
		tableName = mTableName;

		try {
			Class.forName("org.postgresql.Driver");
			if (dbConnectionString != null) {
				connection = DriverManager.getConnection(dbConnectionString, dbUser, dbPasswd);
				// this.connection.setAutoCommit(false);
			}
			loadFileList(filePath);
			loadcsv();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * 加载已有文件
	 * @param strPath
	 */
	public static void loadFileList(String strPath) {
		File dir = new File(strPath);
		File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				String fileName = files[i].getName();
				if (files[i].isDirectory()) { 
					loadFileList(files[i].getAbsolutePath());
				} else if (fileName.toLowerCase().endsWith("csv")) {
					queue.offer(files[i].getAbsolutePath());
				} else {
					continue;
				}
			}
		}
	}

	/**
	 * 将已有csv文件入库
	 */
	public void loadcsv() {
		new Thread() {

			@Override
			public void run() {
				String file = null;
				try {
					while ((file = queue.take()) != null) {
						copyFromFile(connection, file, tableName);
					}
				} catch (InterruptedException | SQLException | IOException e) {
					e.printStackTrace();
				}
			}

		}.start();

	}

	/**
	 * 清空队列
	 */
	public void clear() {
		queue.clear();
	}

	public static void main(String[] args) {
		ResourceListener r = new ResourceListener(null, null, null, "aaa","");

		try {
			queue.put("test1");
			queue.put("test2");
			queue.put("test3");
			Thread.sleep(5000);
			queue.put("test4");
			queue.put("test5");

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void onStart(FileAlterationObserver observer) {
		// System.out.println("onStart");
	}

	@Override
	public void onDirectoryCreate(File directory) {
		// System.out.println("onDirectoryCreate:" + directory.getName());
	}

	@Override
	public void onDirectoryChange(File directory) {
		// System.out.println("onDirectoryChange:" + directory.getName());
	}

	@Override
	public void onDirectoryDelete(File directory) {
		// System.out.println("onDirectoryDelete:" + directory.getName());
	}

	/**
	 * 当有新文件时，加入队列
	 */
	@Override
	public void onFileCreate(File file) {
		// System.out.println("onFileCreate:" + file.getAbsolutePath());
		if (file.getAbsolutePath().toLowerCase().endsWith(".csv")) {
			queue.offer(file.getAbsolutePath());
		}
	}

	@Override
	public void onFileChange(File file) {
		// System.out.println("onFileChange : " + file.getName());
	}

	@Override
	public void onFileDelete(File file) {
		// System.out.println("onFileDelete :" + file.getName());
	}

	@Override
	public void onStop(FileAlterationObserver observer) {

	}

	/**
	 * 关闭连接
	 */
	public void close() {
		System.out.println("onStop");
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行文件写入pg数据库
	 * @param connection
	 * @param filePath
	 * @param tableName
	 * @throws SQLException
	 * @throws IOException
	 */
	private synchronized void copyFromFile(Connection connection, String filePath, String tableName)
			throws SQLException, IOException {
		FileInputStream fileInputStream = null;

		try {
			if (connection != null) {
				String sql = "COPY " + tableName + " FROM STDIN WITH DELIMITER ',' NULL as ' ' CSV QUOTE as '' ESCAPE as '\n'";
				CopyManager copyManager = new CopyManager((BaseConnection) connection);
				fileInputStream = new FileInputStream(filePath);
				copyManager.copyIn(sql, fileInputStream);
			}
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
