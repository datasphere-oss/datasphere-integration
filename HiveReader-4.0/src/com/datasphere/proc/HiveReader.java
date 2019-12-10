package com.datasphere.proc;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.security.Password;
import com.datasphere.proc.events.HDEvent;


@PropertyTemplate(name = "HiveReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "ConnectionString", type = String.class, required = true, defaultValue = "jdbc:hive2://127.0.0.1:10000/default"),
		@PropertyTemplateProperty(name = "osUsername", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "osPassword", type = Password.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Query", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "500")

}, inputType = HDEvent.class)
public class HiveReader extends HiveSecureVerReader {
	private static Logger logger = Logger.getLogger(HiveReader.class);
	

	class SecurityAccess {
		public void disopen() {

		}
	}
}
