package com.datasphere.proc;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.SystemException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.meta.MetaInfo.Target;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.common.*;
import com.datasphere.proc.entity.EventLog;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.proc.utils.PostgresqlUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
/**
 * 
 * @author 目标端 HBase 写入
 *
 */
@PropertyTemplate(name = "HBaseWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "HBaseConfigurationPath", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Columns", type = String.class, required = false, defaultValue = "",label="列名称列表",description="以逗号分隔的字段列表"),
		@PropertyTemplateProperty(name = "FamilyNames", type = String.class, required = true, defaultValue = "") }, outputType = HDEvent.class)
public class HBaseWriter extends BaseProcess {
	private static final Logger logger = Logger.getLogger(HBaseWriter.class);
	private String hbaseConfigurationPath = "";
	private String tables = "";
	private String familyNames = "";
	private String columns = "";
	private boolean wal = false;
//	private Configuration hbaseConfig = null;
//	public static HTablePool pool = null;
//	private HTableInterface table = null;
	private List<Put> list = new ArrayList<Put>();
	private ScheduledThreadPoolExecutor timepool = null;
	private int timeout = 0;
	private Connection connection = null;
	
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
    
	/**
	 * 初始化
	 */
	@Override
	public void init(final Map<String, Object> properties, final Map<String, Object> formatterProperties,
			final UUID inputStream, final String distributionID) throws Exception {
		super.init((Map) properties, (Map) formatterProperties, inputStream, distributionID);
		this.targetUUID = (UUID) properties.get("TargetUUID");

		this.exceptionLogUtils = new PostgresqlUtils();
		try {
			Target current_stream = (Target) MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUUID, HDSecurityManager.TOKEN);
			this.appUUID = current_stream.getCurrentApp().getUuid();
			this.appName = current_stream.getCurrentApp().getName();
		} catch (MetaDataRepositoryException e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "MetaDataRepositoryException", e.getMessage(),eventLog);
			e.printStackTrace();
		}
		Map<String, Object> localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
		localPropertyMap.putAll(properties);
		localPropertyMap.putAll(formatterProperties);
		validateProperties(localPropertyMap);
		
		setup();
		try {
			HBaseAuthUtil.timerLogin();
		}catch(Exception e) {}
		ThreadFactory namedThreadFactory = (Runnable r) -> new Thread(r, "thread_pool_HBaseWriter_" + r.hashCode());
		timepool = new ScheduledThreadPoolExecutor(1,namedThreadFactory);
		/** 记录log */
//		this.eventLog = new EventLog();
//		eventLog = new EventLog();
//		eventLog.setApp_uuid(this.appUUID.getUUIDString());
//		eventLog.setApp_name(this.appName);
//		eventLog.setCluster_name(HazelcastSingleton.getClusterName());
//		eventLog.setNode_id(HazelcastSingleton.getBindingInterface());
//        eventLog.setApp_progress("0%");
		timepool.scheduleAtFixedRate(() -> {
			try {
				if(timeout > 1) {
					if (list.size() > 0) {
						puts(this.tables,list);
						list.clear();
					}
					timeout = 0;
				}
				timeout++;
			} catch (Exception e) {
				e.printStackTrace();
			}
//			if (currentEvent == null && lastEvent != null) {
//	    			/** 更新本批数据同步最终状态 */
//	    			if(flag >= 1) {
//						eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()-5000));
//					} else {
//						eventLog.setEvent_endtime(new Timestamp(System.currentTimeMillis()));
//					}
//					eventLog.setWrite_total(totalData);
//					eventLog.setEvent_total(totalData);// 数据交换总量
//					eventLog.setExce_total(totalExceptionData);
//					eventLog.setApp_status(AppStatus.APP_FINISHED);
//					eventLog.setApp_progress("100%");
//					if (updating && eventLog.getEvent_starttime() != null) {
//						exceptionLogUtils.insertOrUpdate(eventLog);
//					}
//					/** 恢复初始化状态 */
//					lastEvent = null;
//					totalData = 0;
//					updating = false;
//					eventLog.setEvent_starttime(null);
//					eventLog.setEvent_endtime(null);
//					eventLog.setEvent_total(0);
//					eventLog.setWrite_total(0);
//					eventLog.setExce_total(0);
//					eventLog.setApp_status(AppStatus.APP_WAITTING);
//					eventLog.setApp_progress("0%");
//			} else {
//				if (updating && eventLog.getEvent_starttime() != null) {
//					exceptionLogUtils.insertOrUpdate(eventLog); 
//				}
//			}
	    		++flag;
		}, 10, 5 * 1000, TimeUnit.MILLISECONDS);
	}
	/**
	 * 加载属性
	 * @param properties
	 * @throws Exception
	 */
	private void validateProperties(Map<String, Object> properties) throws Exception {
		Property prop = new Property(properties);
		if ((properties.get("HBaseConfigurationPath") != null)
				&& (!properties.get("HBaseConfigurationPath").toString().trim().isEmpty())) {
			this.hbaseConfigurationPath = properties.get("HBaseConfigurationPath").toString().trim();
		}
		if ((properties.get("Tables") != null) && (!properties.get("Tables").toString().trim().isEmpty())) {
			this.tables = properties.get("Tables").toString().trim();
		}
		if ((properties.get("Columns") != null) && (!properties.get("Columns").toString().trim().isEmpty())) {
			this.columns = properties.get("Columns").toString().trim();
		}
		if ((properties.get("FamilyNames") != null) && (!properties.get("FamilyNames").toString().trim().isEmpty())) {
			this.familyNames = properties.get("FamilyNames").toString().trim();
		}
		this.wal = prop.getBoolean("WAL", false);
	}

	/**
	 * 接收处理消息
	 */
	@Override
	public void receive(int channel, Event event) throws Exception {
		receiveImpl(channel, event);
	}

	public void receive(int channel, Event event, Position pos) throws Exception {
		receiveImpl(channel, event);
	}

	@Override
	public void receiveImpl(int channel, Event event) throws Exception {
		try {
			/** 记录log */
			this.currentEvent = event;
//			if(!updating && eventLog.getEvent_starttime() == null) {
//				this.eventLog.setEvent_starttime(new Timestamp(System.currentTimeMillis()));
//				updating = true;
//			}
			if(this.currentEvent == null) {
				return;
			}
			Map<String, Object> map = null;
			if (event instanceof JsonNodeEvent) {
				JsonNodeEvent jsonNode = (JsonNodeEvent) event;
				JsonNode node = jsonNode.getData();
				if (node != null) {
					map = new HashMap<String, Object>();
					JsonObject jsonObj = new JsonParser().parse(node.toString()).getAsJsonObject();
				    Iterator i$ = jsonObj.entrySet().iterator();
				    while(i$.hasNext()) {
				        Map.Entry entry = (Map.Entry)i$.next();
				        map.put(entry.getKey().toString().toUpperCase(), entry.getValue());
				    }
					putColumn(map);
					map = null;
				}
				this.lastEvent = event;
			} else if (event instanceof HDEvent) {
				HDEvent out = (HDEvent) event;
				if("".equalsIgnoreCase(columns) || "null".equalsIgnoreCase(this.columns)) {
					if(out.metadata.containsKey("ColumnName") && out.metadata.get("ColumnName") != null) {
						this.columns = out.metadata.get("ColumnName").toString();
						System.out.println("源端字段列表："+this.columns);
					}
				}
				putColumn(out.data);
				this.lastEvent = event;
				out = null;
			} else {
				putColumn(event.getPayload());
			}
		} catch (Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
			e.printStackTrace();
			throw new AdapterException("HBase 写入错误："+e.getMessage());
		}
	}

	/**
	 * 生成 Rowkey
	 * @param value
	 * @return
	 */
	private static String getRowkey(String value) {
		return getMD5(value);
	}

	@Override
	public void close() throws Exception {
		if (this.exceptionLogUtils != null) this.exceptionLogUtils.close();
		if (this.timepool != null) {
			this.timepool.shutdown();
			this.timepool.purge();
		}
		timeout = 0;
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
			e.printStackTrace();
		}
		try {
			HBaseAuthUtil.closeTimer();
		}catch(Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
		}
	}

	/**
	 * 设置 HBase
	 * @throws Exception
	 */
	private void setup() throws Exception {
		try {
			HBaseAuthUtil.init(this.hbaseConfigurationPath);
//			hbaseConfig = HBaseAuthUtil.getConfigInstance();
			Thread.sleep(2000);
			connection = HBaseAuthUtil.getConnection();
			if(connection == null) {
				System.out.println("----connection:"+connection);
			}
		} catch (Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", "HBase-用户验证失败："+e.getMessage(),eventLog);
			e.printStackTrace();
			throw new AdapterException("HBase 用户验证失败："+e.getMessage());
		}
	}

	/**
	 * 插入数据
	 * @param objects
	 * @throws SystemException 
	 */
	public void putColumn(Object[] objects) throws AdapterException{
		String[] cols = this.columns.split(",");
		if (cols.length <= objects.length) {
			String rowkey = "";
			for (int i = 0; i < objects.length; i++) {
				rowkey += objects[i];
			}
			rowkey = getRowkey(rowkey);
			try {
//				table = pool.getTable(this.tables);
//				table.setAutoFlush(false);
//				table.setWriteBufferSize(24 * 1024 * 1024);
				int count = cols.length;
				for (int i = 0; i < count; i++) {
					String qualifier = cols[i];
					String data = String.valueOf(objects[i]);
					Put put = new Put(Bytes.toBytes(rowkey));
					put.setWriteToWAL(this.wal);
					put.addColumn(Bytes.toBytes(this.familyNames), Bytes.toBytes(qualifier),
							Bytes.toBytes(data != null ? data.toString() : ""));
					list.add(put);
					if (list.size() % 20000 == 0) {
						timeout = 0;
//						table.put(list);
//						table.flushCommits();
						puts(this.tables,list);
						totalData+=20000;
						list.clear();
					}
				}
				timeout = 0;
			} catch (Exception e) {
				++totalExceptionData;
				e.printStackTrace();
			}
		} else {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", "列数不一致，字段数为："+cols.length +",数据列数为："+objects.length,eventLog);
			logger.error("列数不一致，字段数为："+cols.length +",数据列数为："+objects.length);
			throw new AdapterException("列数不一致，字段数为："+cols.length +",数据列数为："+objects.length);
		}
	}
	
	public void putColumn(Map<String, Object> map) {
		String rowkey = "";
		try {
			for (String key : map.keySet()) {
				rowkey += map.get(key);
			}
			rowkey = getRowkey(rowkey);
//			table = pool.getTable(this.tables);
//			table.setAutoFlush(false);
//			table.setWriteBufferSize(24 * 1024 * 1024);
			for (String key : map.keySet()) {
				String value = map.get(key)==null?"":map.get(key).toString();
				String qualifier = key;
				Put put = new Put(Bytes.toBytes(rowkey));
				put.setWriteToWAL(this.wal);
				put.addColumn(Bytes.toBytes(this.familyNames), Bytes.toBytes(qualifier),
						Bytes.toBytes(value != null ? value.toString() : ""));
				list.add(put);
				if (list.size() % 20000 == 0) {
					timeout = 0;
					puts(this.tables,list);
					this.totalData+=20000;
					list.clear();
				}
			}
			timeout = 0;
		} catch (Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
			++totalExceptionData;
			e.printStackTrace();
		}
	}

	public void puts(String tableName,List<Put> puts) throws IOException{
		try {
		 Table table = connection.getTable(TableName.valueOf(tableName));
		 table.put(puts);
		}catch(Exception e) {
			exceptionLogUtils.addException(this.appUUID.toString(), this.appName, HazelcastSingleton.getBindingInterface(), new Timestamp(System.currentTimeMillis()), "Exception", e.getMessage(),eventLog);
			e.printStackTrace();
		}
	}
	
	/**
	 * 对字符串md5加密(小写+字母)
	 *
	 * @param str
	 *            传入要加密的字符串
	 * @return MD5加密后的字符串
	 */
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
	
	public static void main(String[] args) {
		System.out.println(getRowkey("中").length());
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
