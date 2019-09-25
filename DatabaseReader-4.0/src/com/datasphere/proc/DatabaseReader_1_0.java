package com.datasphere.proc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.exc.ConnectionException;
import com.datasphere.event.Event;
import com.datasphere.intf.SourceMetadataProvider;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.components.Flow;
import com.datasphere.security.Password;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.sun.management.OperatingSystemMXBean;

@PropertyTemplate(name = "DatabaseReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "Username", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = "",description = "jdbc:mysql://127.0.0.1:3306/testdb?useUnicode=true&characterEncoding=UTF-8"),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "ExcludedTables", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Query", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "ReturnDateTimeAs", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "15000"),
		@PropertyTemplateProperty(name = "ValidateField", type = String.class, required = false, defaultValue = "")}, inputType = HDEvent.class)
public class DatabaseReader_1_0 extends SourceProcess implements SourceMetadataProvider {
	private static Logger logger;
	private boolean eofWait;
	private DatabaseReader dr;

	private boolean memAvailable = true;//剩余内存是否大于500M
	private OperatingSystemMXBean mem = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

	public DatabaseReader_1_0() {
		this.eofWait = true;
		this.dr = null;
	}

	@Override
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow) throws Exception {
        super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
	}
	
	@Override
	public void init(final Map<String, Object> properties) throws Exception {
		super.init(properties);
		if (properties.containsKey("noeofwait")) {
			this.eofWait = false;
		}

		String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "attach.properties";
		boolean disableStartApp = Boolean.parseBoolean(getProperty("DisableStartApp","false",path));
		if(disableStartApp) {//如果是禁止启动则不执行数据库初始化
			return;
		}
		
		this.dr = new DatabaseReaderOld(isSendColumnName);
		this.dr.initDR(properties);
		System.out.println("----init");
	}
//	public synchronized void init(final Map<String, Object> properties, final Map<String, Object> properties2,
//			final UUID sourceUUID, final String distributionId) throws Exception {
//		super.init(properties, properties2, sourceUUID, distributionId);
//		properties.put("sourceUUID", sourceUUID);
//		if (properties.containsKey("noeofwait")) {
//			this.eofWait = false;
//		}
////		if (properties.containsKey("Query") && properties.get("Query") != null
////				&& properties.get("Query").toString().length() > 0) {
//			this.dr = new DatabaseReaderOld();
////		} else {
////			this.dr = new DatabaseReaderNew();
////		}
//		this.dr.initDR(properties);
//		System.out.println("----init2");
//	}

	public void receiveImpl(final int channel, final Event event) throws Exception {
		String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "attach.properties";
		boolean disableStartApp = Boolean.parseBoolean(getProperty("DisableStartApp","false",path));
		if(disableStartApp) {//如果是禁止启动则发送空消息
			this.send((Event) null, 0);
			return;
		}
		try {
			long availableMem = mem.getFreePhysicalMemorySize() / 1024 / 1024;
	    		if(availableMem < 300) {
	    			memAvailable = false;
	    			availableMem = mem.getFreePhysicalMemorySize() / 1024 / 1024;
	    			System.out.println("---内存不足，暂停发送数据，当前可用内存：" +availableMem+ "MB");
	    			System.gc();
	    			try {
	    				Thread.sleep(60 * 1000);
	    			}catch(Exception e) {
	    				
	    			}
	    			return;
	    		}
	    		if(!memAvailable) {
	    			memAvailable = true;
	    			System.out.println("---内存已恢复到可用状态，当前可用内存：" +availableMem+ "MB");
	    		}
			final HDEvent evt = this.dr.nextEvent(this.sourceUUID);
			if (evt != null) {
				if (this.sourceUUID != null && this.typeUUIDForCDCTables != null) {
					evt.typeUUID = super.typeUUIDForCDCTables.get(this.dr.getCurrentTableName());
				}
				evt.metadata.put("status", "Syncing");
				this.send((Event) evt, 0);
			} else {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException ie) {
					this.close();
					throw new ConnectionException("Stopping the Source thread has been interrupted.");
				}
			}
		} catch (NoSuchElementException ex) {
			try {
				if (this.eofWait) {
					Thread.sleep(100L);
				} else {
					final HDEvent out = new HDEvent(0, this.sourceUUID);
					out.metadata.put("status", "Completed");
					out.data = null;
					this.send((Event) out, 0);
				}
			} catch (InterruptedException ie) {
				this.close();
				if (!logger.isInfoEnabled()) {
					return;
				}
				logger
						.info((Object) "DatabaseReader stopping gracefully as the thread has been interrupted");
			}
		}
	}

	public void close() throws Exception {
		this.dr.close();
		super.close();
	}

	public static void main(final String[] args) throws Exception {
		final Map<String, Object> props = new HashMap<String, Object>();
		props.put("ConnectionURL", "jdbc:oracle:thin:@192.168.123.145:1521/XE");
		props.put("Username", "scott");
		props.put("Password", "tiger");
		props.put("FetchSize", 2000);
		props.put("Tables", " SCOTT.Common%");
		props.put("ExcludedTables", "AUTHORIZATIONSOLD");
		DatabaseReader_1_0 source = new DatabaseReader_1_0();
		
		try {
			source.init(props, props, new UUID(System.currentTimeMillis()), "qqqq");
//			source.addEventSink((EventSink) new AbstractEventSink() {
//				public void receive(final int channel, final Event event) throws Exception {
//					System.out.println(event.toString());
//				}
//			});
//			for (int i = 0; i < 10; ++i) {
//				source.receive(0, (Event) null);
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getMetadataKey() {
		return this.dr.getMetadataKey();
	}

	public Map<String, TypeDefOrName> getMetadata() throws Exception {
		return (Map<String, TypeDefOrName>) this.dr.getMetadata();
	}

	static {
		DatabaseReader_1_0.logger = Logger.getLogger(DatabaseReader_1_0.class);
	}
	
	class SecurityAccess {
		public void disopen() {
			
		}
    }
}
