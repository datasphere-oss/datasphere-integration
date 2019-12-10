package com.datasphere.proc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.classloading.HDLoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.event.SimpleEvent;
import com.datasphere.intf.AuthLayer.AuthType;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.meta.MetaInfo.Stream;
import com.datasphere.runtime.meta.MetaInfo.Type;
import com.datasphere.security.Password;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.enums.Mode;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.proc.records.Record;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

@PropertyTemplate(name = "MongoWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "connectionUrl", type = String.class, required = true, defaultValue = "localhost:27017"),
		@PropertyTemplateProperty(name = "collections", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "excludeCollections", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "mode", type = Mode.class, required = false, defaultValue = "InitialLoad"),
		@PropertyTemplateProperty(name = "userName", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "password", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "authType", type = AuthType.class, required = false, defaultValue = "Default"),
		@PropertyTemplateProperty(name = "authDB", type = String.class, required = false, defaultValue = "admin"),
		@PropertyTemplateProperty(name = "readPreference", type = ReadPreference.class, required = false, defaultValue = "primaryPreferred"),
		@PropertyTemplateProperty(name = "startTimestamp", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "sslEnabled", type = Boolean.class, required = false, defaultValue = "false"),
		@PropertyTemplateProperty(name = "connectionRetryPolicy", type = String.class, required = false, defaultValue = "retryInterval=30, maxRetries=3") }, outputType = SimpleEvent.class)
public class MongoDBWriter extends BaseProcess {
	private static Logger logger = Logger.getLogger(MongoDBWriter.class);
	private String dbName = null;
	private String collectionName = null;
	private String dbUser = null;
	private String dbPasswd = null;
	private String host = "localhost";
	private int port = 27017;
	public static String USERNAME = "userName";
	public static String PASSWORD = "password";
	public static String COLLECTION_NAME = "collections";
	public static String DATABASE_NAME = "authDB";
	public static String CONNECTION = "connectionUrl";
	boolean isHDEvent = false;
	private Type dataType = null;
	private MongoClient client;
	private MongoDatabase mongoDB;
	private MongoCollection<Document> mongoCollection;

	private String TARGET_OBJECT_EXISTS_ACTION = "drop";

	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID);
		try {
			Stream stream = (Stream) MetadataRepository.getINSTANCE().getMetaObjectByUUID(sourceUUID,
					HDSecurityManager.TOKEN);
			this.dataType = ((Type) MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType,
					HDSecurityManager.TOKEN));

			Class<?> typeClass = WALoader.get().loadClass(this.dataType.className);
			if (typeClass.getSimpleName().equals("HDEvent")) {
				this.isHDEvent = true;
			}
			validateProperties(properties);
		} catch (Exception ex) {
			throw new AdapterException("Error Initializing MongoDBWriter: " + ex.getMessage());
		}
	}

	private void validateProperties(Map<String, Object> properties) throws Exception {
		if ((properties.get(DATABASE_NAME) != null) && (!properties.get(DATABASE_NAME).toString().trim().isEmpty())) {
			this.dbName = properties.get(DATABASE_NAME).toString().trim();
		} else {
			throw new IllegalArgumentException("Database name must be provided");
		}
		if ((properties.get(COLLECTION_NAME) != null)
				&& (!properties.get(COLLECTION_NAME).toString().trim().isEmpty())) {
			this.collectionName = properties.get(COLLECTION_NAME).toString().trim();
		} else {
			throw new IllegalArgumentException("Collection name must be provided");
		}
		if ((properties.containsKey(CONNECTION)) && (properties.get(CONNECTION) != null)) {
			String val = properties.get(CONNECTION).toString();

			String[] split = val.split(":");
			if ((!val.contains(":")) || (split[0].trim().equals("")) || (split[1].trim().equals(""))) {
				throw new IllegalArgumentException(
						"Connection string specified " + val + " is invalid. It must be specified as host:port");
			}
			try {
				InetAddress.getByName(split[0].trim());
			} catch (UnknownHostException ex) {
				throw new UnknownHostException("Connection String invalid at host :" + split[0]);
			}
			this.host = split[0].trim();
			try {
				this.port = Integer.parseInt(split[1].trim());
			} catch (NumberFormatException ex) {
				throw new NumberFormatException("Connection String invalid at port :" + split[1]);
			}
		}
		if ((properties.containsKey(USERNAME)) && (properties.get(USERNAME) != null)) {
			this.dbUser = properties.get(USERNAME).toString();
		}
		if ((properties.containsKey(PASSWORD)) && (properties.get(PASSWORD) != null)) {
			this.dbPasswd = ((Password) properties.get(PASSWORD)).getPlain().toString();
		}
		// if((properties.get(TARGET_OBJECT_EXISTS_ACTION) != null) &&
		// (!properties.get(TARGET_OBJECT_EXISTS_ACTION).toString().trim().isEmpty())){
		// this.targetObjectExistsAction =
		// properties.get(TARGET_OBJECT_EXISTS_ACTION).toString();;
		// }
		open();
	}

	public void open() throws Exception {
		if (this.dbUser != null) {
			MongoCredential credential = MongoCredential.createCredential(this.dbUser, this.dbName.trim(),
					this.dbPasswd.toCharArray());
			this.client = new MongoClient(new ServerAddress(this.host, this.port),
					Arrays.asList(new MongoCredential[] { credential }));
		} else {
			this.client = new MongoClient(this.host, this.port);
		}
		boolean dbFound = false;
		MongoCursor<String> listDatabaseNames = this.client.listDatabaseNames().iterator();
		while (listDatabaseNames.hasNext()) {
			String val = (String) listDatabaseNames.next();
			if (val.equals(this.dbName.trim())) {
				dbFound = true;
				break;
			}
		}
		if (dbFound) {
			this.mongoDB = this.client.getDatabase(this.dbName.trim());
		} else {
			throw new AdapterException("在数据库" + this.dbName + " 上查询发生错误! 请指定一个有效的数据库名称");
		}
		if (logger.isInfoEnabled()) {
			logger.info("已经成功连接到 " + this.dbName + " 数据库");
		}
		MongoCursor<String> listCollectionNames = this.mongoDB.listCollectionNames().iterator();
		boolean collectionFound = false;
		while (listCollectionNames.hasNext()) {
			String val = (String) listCollectionNames.next();
			if (val.equals(this.collectionName.trim())) {
				collectionFound = true;
				break;
			}
		}
		if (collectionFound) {
			this.mongoCollection = this.mongoDB.getCollection(this.collectionName.trim());

			// switch (this.targetObjectExistsAction)
			// {
			// case "drop":
			// try
			// {
			// logger.debug("同步数据 - 发现目标对象 " + this.collectionName + ". 删除所有文档 [drop 模式]");
			// mongoCollection.drop();
			// }
			// catch (MongoException mongoEx)
			// {
			// logger.error("当删除集合 " + this.collectionName + " 时, 发生错误. 错误: " +
			// mongoEx.getMessage());
			// throw mongoEx;
			// }
			//
			//
			//
			// case "append":
			// this.logger.debug("同步 - 发现目标对象 " + this.collectionName + ". 添加新的记录 [append
			// 模式]");
			// }

		} else {
			throw new AdapterException("当查询集合 " + this.collectionName + " 时发生错误. 请指定一个有效的集合名称");
		}
		if (logger.isInfoEnabled()) {
			logger.info("集合 " + this.collectionName + " 查询成功! ");
		}
	}

	public void receiveImpl(int channel, Event event) throws Exception {
		try {
			HDEvent we = (HDEvent) event;
			if(((HDEvent) event).metadata.get("OperationName") != null && (((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("HBASE_READER"))) {
				String rowkey = ((HDEvent) event).metadata.get("rowkey").toString();
				String qualifier = ((HDEvent) event).metadata.get("qualifier").toString();
				String data = ((HDEvent) event).metadata.get("data").toString();
				Document doc = new Document();
				doc.append(qualifier, data);
				insertToDatabase(doc);
			}else if (this.isHDEvent) {
				if (we.typeUUID == null) {
					Object[] data = we.data;
					Document document = new Document();
					for (int i = 0; i < data.length; i++) {
						document.append(i + "", data[i]);
					}
					insertToDatabase(document);
					return;
				}else if ((((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("INSERT"))) {
					handleInsertEvent(we);

				} else if ((((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("UPDATE"))) {
					handleUpdateEvent(we);

				} else if ((((HDEvent) event).metadata.get("OperationName").toString().equalsIgnoreCase("DELETE"))) {
					handleDeleteEvent(we);

				}
			}else {
				Map<String, Object> data = getSimpleData((SimpleEvent) event);
				handleDataEvent(data);
			}
			// handleHDEvent(we);
		} catch (Exception ex) {
			throw new AdapterException("Error writing to db " + this.dbName + " with collectionName "
					+ this.collectionName + ": " + ex.getMessage());
		}
	}

	public void insertToDatabase(Document document) {
		synchronized (this.mongoCollection) {
			this.mongoCollection.insertOne(document);
		}
	}

	public void updateToDatabase(Document docBefore, Document docAfter) {
		synchronized (this.mongoCollection) {
			this.mongoCollection.replaceOne(docBefore, docAfter);
		}
	}

	public void deleteToDatabase(Document document) {
		synchronized (this.mongoCollection) {
			this.mongoCollection.deleteOne(document);
		}
	}

	private Map<String, Object> getSimpleData(SimpleEvent e) {
		Map<String, String> keys = this.dataType.fields;
		Map<String, Object> result = new HashMap();
		Object[] data = e.getPayload();
		Iterator<String> iter = keys.keySet().iterator();
		int i = 0;
		while (iter.hasNext()) {
			String key = (String) iter.next();
			result.put(key, data[i]);
//			System.out.println(" 填充的数据为: " + key + ", " + data[i]);

			i++;
		}
		return result;
	}

	private void handleDataEvent(Map<String, Object> event) throws Exception {

		Document document = new Document();

		Set<Map.Entry<String, Object>> set = event.entrySet();
		Iterator<Map.Entry<String, Object>> iter = set.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Object> val = (Map.Entry) iter.next();
			document.append((String) val.getKey(), val.getValue());
//			System.out.println(" 向 mongodb 中插入数据: " + (String) val.getKey() + ", " + val.getValue());
		}
		insertToDatabase(document);
	}

	private void handleInsertEvent(HDEvent we) throws Exception {
		Record evt = BuiltInFunc.convertAvroEventToRecord(we);
		Map<String, Object> event = BuiltInFunc.DATA(evt);

		Document document = new Document();

		Set<Map.Entry<String, Object>> set = event.entrySet();
		Iterator<Map.Entry<String, Object>> iter = set.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Object> val = (Map.Entry) iter.next();
			document.append((String) val.getKey(), val.getValue());
//			System.out.println(" 向 mongodb 中插入数据: " + (String) val.getKey() + ", " + val.getValue());
		}

		insertToDatabase(document);

	}

	private void handleUpdateEvent(HDEvent we) throws Exception {
		Record evt = BuiltInFunc.convertAvroEventToRecord(we);
		Map<String, Object> dataAfter = BuiltInFunc.DATA(evt);
		Map<String, Object> dataBefore = BuiltInFunc.BEFORE(evt);

		Document docAfter = new Document();
		Document docBefore = new Document();

		Set<Map.Entry<String, Object>> setAfter = dataAfter.entrySet();
		Iterator<Map.Entry<String, Object>> iterAfter = setAfter.iterator();
		while (iterAfter.hasNext()) {
			Map.Entry<String, Object> val = (Map.Entry) iterAfter.next();
			docAfter.append((String) val.getKey(), val.getValue());
//			System.out.println(" 向 mongodb 中插入当前数据: " + (String) val.getKey() + ", " + val.getValue());
		}

		Set<Map.Entry<String, Object>> set = dataBefore.entrySet();
		Iterator<Map.Entry<String, Object>> iterBefore = set.iterator();
		while (iterBefore.hasNext()) {
			Map.Entry<String, Object> val = (Map.Entry) iterBefore.next();
			docBefore.append((String) val.getKey(), val.getValue());
//			System.out.println(" 向 mongodb 中插入更新之前的数据: " + (String) val.getKey() + ", " + val.getValue());
		}

		updateToDatabase(docBefore, docAfter);

	}

	private void handleDeleteEvent(HDEvent we) throws Exception {
		Record evt = BuiltInFunc.convertAvroEventToRecord(we);
		Map<String, Object> event = BuiltInFunc.DATA(evt);

		Document document = new Document();

		Set<Map.Entry<String, Object>> set = event.entrySet();
		Iterator<Map.Entry<String, Object>> iter = set.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Object> val = (Map.Entry) iter.next();
			document.append((String) val.getKey(), val.getValue());
//			System.out.println(" 向 mongodb 中删除数据: " + (String) val.getKey() + ", " + val.getValue());
		}

		deleteToDatabase(document);

	}

	public void close() throws Exception {
		this.client.close();
	}

	public static String getMongoType(String sourceType) {
		String mongoType = "";
		switch (sourceType.toUpperCase()) {
		case "VARCHAR2":
		case "NVARCHAR2":
		case "NCHAR VARYING":
		case "VARCHAR":
		case "CHAR":
		case "NCHAR":
		case "NVARCHAR":
		case "SYSNAME":
		case "CLOB":
		case "MONEY":
		case "SMALLMONEY":
		case "TEXT":
		case "NTEXT":
		case "GRAPHIC":
		case "VARGRAPHIC":
		case "VARG":
		case "CHARACTER":
		case "UNIQUEIDENTIFIER":
		case "DECFLOAT":
			mongoType = "String";
			break;
		case "TINYINT":
		case "SMALLINT":
		case "INT":
		case "BIGINT":
		case "FLOAT":
		case "REAL":
		case "DECIMAL":
		case "NUMERIC":
		case "NUMBER":
		case "INTEGER":
		case "DOUBLE":
			mongoType = "Double";
			break;
		case "DATE":
		case "DATETIME":
		case "TIME":
		case "TIME WITH TIME ZONE":
		case "TIMESTAMP":
		case "TIMESTAMP WITH TIME ZONE":
		case "TIMESTAMP WITH LOCAL TIME ZONE":
		case "TIMESTMP":
			mongoType = "Date";
		}

		return mongoType;
	}

	public static void main(String[] args) throws Exception {
		HashMap<String, Object> writerProperties = new HashMap();
		writerProperties.put(DATABASE_NAME, "my_local");
		writerProperties.put(COLLECTION_NAME, "mycollection1");
		try {
			MongoDBWriter dbWriter = new MongoDBWriter();
			dbWriter.validateProperties(writerProperties);
			System.out.println(dbWriter.host + ":" + dbWriter.port);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
