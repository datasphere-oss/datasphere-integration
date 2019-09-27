package com.datasphere.proc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.event.SimpleEvent;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "HBaseReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "HBaseConfigurationPath", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "FamilyNames", type = String.class, required = true, defaultValue = "") }, inputType = SimpleEvent.class)
public class HBaseReader extends SourceProcess {
	private static final Logger logger = Logger.getLogger(HBaseReader.class);
	private String hbaseConfigurationPath = "";
	private String tables = "";
	private String familyNames = "";
	// 与HBase数据库的连接对象
	private static Connection connection;
	// 数据库元数据操作对象
	private Admin admin;
	private boolean bRun = false;

	@Override
	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
		Property prop = new Property(properties);
		this.hbaseConfigurationPath = prop.getString("HBaseConfigurationPath", "");
		this.tables = prop.getString("Tables", "");
		this.familyNames = prop.getString("FamilyNames", "");
		setup();
		HBaseAuthUtil.timerLogin();
	}

	@Override
	public void receiveImpl(int arg0, Event arg1) throws Exception {
		if (!bRun) {
			bRun = true;
			queryTable();
		}
	}

	@Override
	public void close() throws Exception {
		cleanUp();
		bRun = false;
	}

	/**
	 * 设置 HBase
	 * @throws Exception
	 */
	private void setup() throws Exception {
		try {
			HBaseAuthUtil.init(this.hbaseConfigurationPath);
			Thread.sleep(2000);
			connection = HBaseAuthUtil.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("HBase 用户验证失败："+e.getMessage());
		}
	}

	// 关闭连接
	private void cleanUp() {
		try {
			if (connection != null)
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		HBaseAuthUtil.closeTimer();
	}

	/**
	 * 查询整表数据
	 */
	private void queryTable() throws Exception {
		TableName tname = TableName.valueOf(this.tables);
		// 取得数据表对象
		Table table = connection.getTable(tname);
		Scan scan = new Scan();
		scan.addFamily(this.familyNames.getBytes());
		// 取得表中所有数据
		ResultScanner scanner = table.getScanner(scan);

		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				HDEvent out = new HDEvent(0, this.sourceUUID);
				out.metadata = new HashMap();
				out.metadata.put("OperationName", "HBASE_READER");
				out.metadata.put("rowkey", Bytes.toString(CellUtil.cloneRow(cell)));
				out.metadata.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));
				out.metadata.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));
				out.metadata.put("data", Bytes.toString(CellUtil.cloneValue(cell)));
				Object[] obj = new Object[1];
				obj[0] = Bytes.toString(CellUtil.cloneValue(cell));
				out.data = obj;
				send((Event) out, 0);
				try {
					Thread.sleep(100);
				} catch (Exception e) {

				}
			}
		}
	}

	public boolean putColumn(String tableName, String familyName, String rowkey, String column, String val)
			throws Exception {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			if (StringUtils.isNotEmpty(val)) {
				Put put = new Put(Bytes.toBytes(rowkey));
				addColumn(put, familyName, column, val);
				table.put(put);
			}
		} catch (Exception e) {
			logger.error("HbaseService get error" + e.getMessage());
		}
		return true;
	}

	private Put addColumn(Put put, String familyName, String column, String value) {
		put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column),
				Bytes.toBytes(value != null ? value.toString() : ""));
		return put;
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
