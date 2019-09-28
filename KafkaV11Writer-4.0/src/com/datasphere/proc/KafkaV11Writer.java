package com.datasphere.proc;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "KafkaWriter", version = "0.11.0", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "brokerAddress", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Topic", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Mode", type = String.class, required = false, defaultValue = "Async"),
		@PropertyTemplateProperty(name = "KafkaConfig", type = String.class, required = false, defaultValue = "request.timeout.ms=60001;session.timeout.ms=60000"),
		@PropertyTemplateProperty(name = "ParallelThreads", type = int.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "PartitionKey", type = String.class, required = false, defaultValue = "") }, outputType = HDEvent.class, requiresFormatter = true, isParallelizable = true)
public class KafkaV11Writer extends KafkaV10WriterImpl {
	@Override
	public int getVersion() {
		return 11;
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
