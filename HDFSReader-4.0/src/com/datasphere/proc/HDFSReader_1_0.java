package com.datasphere.proc;

import com.datasphere.anno.*;

import java.util.*;
import java.io.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.reader.*;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.io.hdfs.reader.*;
import com.datasphere.proc.events.*;

@PropertyTemplate(name = "HDFSReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "hadoopurl", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "wildcard", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "positionbyeof", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "eofdelay", type = Integer.class, required = false, defaultValue = "100"),
		@PropertyTemplateProperty(name = "skipbom", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "rolloverstyle", type = String.class, required = false, defaultValue = "Default"),
		@PropertyTemplateProperty(name = "compressiontype", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "authenticationpolicy", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "hadoopconfigurationpath", type = String.class, required = false, defaultValue = "") }, inputType = HDEvent.class, requiresParser = true)
public class HDFSReader_1_0 extends BaseReader {
	public InputStream createInputStream(final Map<String, Object> prop, final boolean recoveryMode) throws Exception {
		final Property property = new Property((Map) prop);
		return (InputStream) Reader.createInstance((ReaderBase) new HDFSReader(property), property, recoveryMode);
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
