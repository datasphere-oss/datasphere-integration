package com.datasphere.proc;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.security.Password;
import com.datasphere.proc.events.HDEvent;

/**
 * 
 * 
 * hive/conf/hive-site.xml <property> <name>hadoop.proxyuser.root.hosts</name>
 * <value>*</value> </property> <property>
 * <name>hadoop.proxyuser.root.groups</name> <value>*</value> </property>
 * 
 * //华为jdbc:
 * jdbc:hive2://ha-cluster/default;zk.quorum=192.168.0.101,192.168.0.102,192.168.0.103;zk.port=24002;user.principal=hive/hadoop.hadoop.com;user.keytab=conf/hive.keytab;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM
 *
 */
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
