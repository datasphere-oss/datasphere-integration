package com.datasphere.proc;

import java.util.HashMap;
import java.util.Map;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.event.Event;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.security.Password;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "PostgreSQLCopyReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Username", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Directory", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = "") }, inputType = HDEvent.class)
public class PostgreSQLCopyReader extends SourceProcess {

	private String tableName = null;
	private String dbConnectionString = null;
	private String dbUser = null;
	private String dbPasswd = null;
	private String directory = null;

	private boolean ispgcopy = true;
	private ZJPFileMonitor monitor = null;
	private ResourceListener resourceListener = null;

	@Override
	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
		this.directory = properties.get("Directory").toString().trim();
		this.tableName = properties.get("Tables").toString().replace(";", "").trim();
		this.dbConnectionString = properties.get("ConnectionURL").toString();
		this.dbUser = properties.get("Username").toString();
		this.dbPasswd = ((Password)properties.get("Password")).getPlain().toString();
		ispgcopy = true;
		
	}

	@Override
	public void close() throws Exception {
		try {
			if (monitor != null) {
				monitor.stop();
				monitor = null;
			}
			if(resourceListener != null) {
				resourceListener.clear();
				resourceListener.close();
				resourceListener = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		ispgcopy = true;
	}

	@Override
	public void receiveImpl(int p0, Event p1) throws Exception {
		try {
			if (ispgcopy) {
				ispgcopy = false;
				resourceListener = new ResourceListener(this.dbConnectionString,this.dbUser , this.dbPasswd, this.tableName,this.directory);
				monitor = new ZJPFileMonitor(5000);
				monitor.monitor(this.directory, resourceListener);
				monitor.start();
			}
			send(this.directory);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void send(String filePath) {
		try {
			HDEvent out = new HDEvent(1, this.sourceUUID);
			out.metadata = new HashMap<>();
			out.metadata.put(Constant.OPERATION, "INSERT");
			out.metadata.put("FileName", tableName);
			out.metadata.put(Constant.TABLE_NAME, tableName);
			out.typeUUID = this.typeUUID;
			out.data = new Object[1];
			out.setData(0, filePath);
			send((Event) out, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
