package com.datasphere.proc;

import com.datasphere.anno.*;
import com.datasphere.io.hdfs.writer.*;
import com.datasphere.proc.events.*;

@PropertyTemplate(name = "HDFSWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "filename", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "hadoopurl", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "directory", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "rolloverpolicy", type = String.class, required = false, defaultValue = "interval:60s"),
		@PropertyTemplateProperty(name = "flushpolicy", type = String.class, required = false, defaultValue = "eventcount:10000,interval:30"),
		@PropertyTemplateProperty(name = "authenticationpolicy", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "hadoopconfigurationpath", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "compressiontype", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "rolloveronddl", type = Boolean.class, required = false, defaultValue = "true") }, outputType = HDEvent.class, requiresFormatter = true)
public class HDFSWriter_1_0 extends HDFSWriterImpl {

	class SecurityAccess {
		public void disopen() {

		}
	}
}