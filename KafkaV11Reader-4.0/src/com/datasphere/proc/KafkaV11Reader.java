package com.datasphere.proc;

import com.datasphere.anno.*;

import org.apache.log4j.*;
import org.apache.kafka.common.*;
import com.datasphere.source.lib.type.*;
import org.apache.kafka.clients.consumer.*;
import java.util.*;
import com.datasphere.source.kafka.*;
import org.joda.time.format.*;
import org.joda.time.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.components.*;
import com.datasphere.event.*;

@PropertyTemplate(name = "KafkaReader", version = "0.11.0", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "brokerAddress", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "Topic", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "PartitionIDList", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "startOffset", type = Integer.class, required = false, defaultValue = "-1"),
		@PropertyTemplateProperty(name = "startTimestamp", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "AutoMapPartition", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "KafkaConfig", type = String.class, required = false, defaultValue = "max.partition.fetch.bytes=10485760;fetch.min.bytes=1048576;fetch.max.wait.ms=1000;receive.buffer.bytes=2000000;poll.timeout.ms=10000;request.timeout.ms=60001;session.timeout.ms=60000") }, inputType = HDEvent.class, requiresParser = true)
public class KafkaV11Reader extends KafkaReader {
	private static final Logger logger;

	@Override
	public void setConsumerPosition(final List<TopicPartition> tpList, final int totalPartitionCount) {
		if (this.posType == positiontype.HD_POSITION_OFFSET) {
			final Map<TopicPartition, Long> beginOffsets = this.oneConsumerForAllPartitions.beginningOffsets(tpList);
			final Map<TopicPartition, Long> lastOffsets = this.oneConsumerForAllPartitions.endOffsets(tpList);
			this.oneConsumerForAllPartitions.seekToEnd(new ArrayList<TopicPartition>());
			for (final TopicPartition tp : tpList) {
				long kafkaReadOffset = 0L;
				KafkaSourcePosition sp = null;
				String did = super.topic + "-" + tp.partition();
				sp = (KafkaSourcePosition) this.kafkaSourcePositions.get(did);
				if (sp == null) {
					did = super.distributionID + ":" + this.topic + "-" + tp.partition();
					sp = (KafkaSourcePosition) this.kafkaSourcePositions.get(did);
				}
				if (sp != null) {
					kafkaReadOffset = sp.getKafkaReadOffset();
					if (kafkaReadOffset > lastOffsets.get(tp) || kafkaReadOffset < beginOffsets.get(tp)) {
						throw new RuntimeException(
								"Offset " + kafkaReadOffset + " is out of range for  " + tp + ".Valid begin offset is "
										+ beginOffsets.get(tp) + " and Latest offset is " + lastOffsets.get(tp));
					}
					super.oneConsumerForAllPartitions.seek(tp, kafkaReadOffset);
					this.partitionMap.get(tp.partition()).setRestartPosition(sp);
					if (!KafkaV11Reader.logger.isInfoEnabled()) {
						continue;
					}
					KafkaV11Reader.logger.info((Object) ("Start position available. Kafka Read offset is "
							+ kafkaReadOffset + " for " + this.topic + "-" + tp.partition()));
				} else {
					if (KafkaV11Reader.logger.isInfoEnabled()) {
						KafkaV11Reader.logger.info((Object) ("Restart position is not available for " + this.topic + "-"
								+ tp.partition() + ". Positioning to beginning."));
					}
					super.oneConsumerForAllPartitions.seekToBeginning(Collections.singletonList(tp));
				}
			}
		} else if (this.posType == positiontype.HD_POSITION_EOF) {
			super.oneConsumerForAllPartitions.seekToEnd(new ArrayList<TopicPartition>());
		} else if (this.posType == positiontype.HD_POSITION_SOF) {
			super.oneConsumerForAllPartitions.seekToBeginning(new ArrayList<TopicPartition>());
		} else if (this.posType == positiontype.HD_POSITION_TIMESTAMP) {
			final Map propMap = this.prop.propMap;
			final KafkaProperty prop = this.prop;
			final Object timeObj = propMap.get("startTimestamp");
			long startTimeStamp;
			try {
				if (!(timeObj instanceof String)) {
					throw new RuntimeException("Invalid type " + timeObj.getClass()
							+ ". Expected type is String and format is \"yyyy-MM-dd HH:mm:ss.SSS\"");
				}
				final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
				final DateTime dt = formatter.parseDateTime((String) timeObj);
				startTimeStamp = dt.getMillis();
			} catch (Exception e) {
				throw new RuntimeException("Invalid startTimestamp value  " + timeObj
						+ ". Expected startTimestamp format is yyyy-MM-dd HH:mm:ss.SSS. " + e);
			}
			final Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>(tpList.size());
			for (final TopicPartition tp2 : tpList) {
				timestampsToSearch.put(tp2, startTimeStamp);
			}
			final Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = super.oneConsumerForAllPartitions
					.offsetsForTimes(timestampsToSearch);
			if (offsetAndTimestamp == null || offsetAndTimestamp.isEmpty()) {
				throw new RuntimeException("Invalid startTimestamp value " + timeObj
						+ ". No KakfaMessages found with the given timestamp in " + tpList);
			}
			for (final TopicPartition tp3 : tpList) {
				if (!offsetAndTimestamp.containsKey(tp3)) {
					throw new RuntimeException("No KakfaMessages found with the given timestamp " + timeObj + " in "
							+ tp3
							+ ". Please specify a start timestamp which is common to all partitions or specify respective partitions via partitionIDList property.");
				}
				final Long offset = offsetAndTimestamp.get(tp3).offset();
				if (offset == null) {
					throw new RuntimeException("No KakfaMessages found with the given timestamp " + timeObj + " in "
							+ tp3
							+ ". Please specify a start timestamp which is common to all partitions or specify respective partitions via partitionIDList property.");
				}
				if (KafkaV11Reader.logger.isInfoEnabled()) {
					KafkaV11Reader.logger.info((Object) ("Offset for " + tp3 + " is " + offset
							+ " which is the earliest offset whose timestamp is greater than or equal to "
							+ startTimeStamp));
				}
				super.oneConsumerForAllPartitions.seek(tp3, offset);
			}
		}
	}

	@Override
	public void assignPartitions(final List<TopicPartition> topicPartitions) {
		super.oneConsumerForAllPartitions.assign(topicPartitions);
	}

	public static void main(final String[] args) {
		final HashMap<String, Object> mr = new HashMap<String, Object>();
		mr.put("brokerAddress", "localhost:9092");
		mr.put("startTimeStamp", "2017-10-27 17:51:00.000");
		mr.put("Topic", "test40");
		mr.put("AutoMapPartition", false);
		final HashMap<String, Object> mp = new HashMap<String, Object>();
		mp.put("handler", "DSVParser");
		mp.put("blocksize", "256");
		mp.put("columndelimiter", ",");
		mp.put("rowdelimiter", "\n");
		mp.put("charset", "UTF-8");
		final KafkaV11Reader kf = new KafkaV11Reader();
		System.out.println("Gonna initialize");
		final UUID uuid = new UUID(System.currentTimeMillis());
		mp.put(Property.SOURCE_UUID, uuid);
		try {
			kf.init(mr, mp, uuid, "node10", null, true, null, null);
			kf.receiveImpl(0, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static {
		logger = Logger.getLogger((Class) KafkaV11Reader.class);
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
