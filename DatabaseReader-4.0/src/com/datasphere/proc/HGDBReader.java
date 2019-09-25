package com.datasphere.proc;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.security.Password;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "HGDBReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "Username", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Password", type = Password.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "ConnectionURL", type = String.class, required = true, defaultValue = "jdbc:highgo://127.0.0.1:5866/highgo"),
		@PropertyTemplateProperty(name = "Tables", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "ExcludedTables", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "Query", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "ReturnDateTimeAs", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "FetchSize", type = Integer.class, required = false, defaultValue = "15000"),
		@PropertyTemplateProperty(name = "ValidateField", type = String.class, required = false, defaultValue = "")}, inputType = HDEvent.class)
public class HGDBReader extends DatabaseReader_1_0 {
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
