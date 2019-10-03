package com.datasphere.source.kafka.utils;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.log4j.Logger;

import com.datasphere.common.exc.ConnectionException;
import com.datasphere.common.exc.MetadataUnavailableException;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaUtils {
	private static final Logger logger;

	public static long getPartitionOffset(final String topic, final int partition, final long whichTime,
			final String ipAddress, final int port) throws Exception {
		final String clientName = "Client_" + topic + "_" + partition;
		final SimpleConsumer consumer = new SimpleConsumer(ipAddress, port, 30010, 65536, clientName + "OffsetLookUp");
		final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		final OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(), clientName);
		final OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			final short code = response.errorCode(topic, partition);
			final Throwable cause = ErrorMapping.exceptionFor(code);
			final KafkaException k = new KafkaException(
					"Error fetching Offset from the Broker [" + ipAddress + ":" + port + "]", cause);
			KafkaUtils.logger.warn((Object) (k.getCause() + " " + k.getMessage()));
			throw k;
		}
		final long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	public static TopicMetadata lookupTopicMetadata(final String ipAddress, final int port, final String topic,
			final long retryBackoffms) throws Exception {
		SimpleConsumer consumer = null;
		TopicMetadata tm = null;
		for (int i = 0; i < 10 && tm == null; ++i) {
			try {
				consumer = new SimpleConsumer(ipAddress, port, 30010, 65536, "TopicMetadataLookUp");
				final List<String> topics = Collections.singletonList(topic);
				final TopicMetadataRequest req = new TopicMetadataRequest(topics);
				final TopicMetadataResponse resp = consumer.send(req);
				final List<TopicMetadata> metaData = resp.topicsMetadata();
				for (final TopicMetadata topicMetadata : metaData) {
					if (topicMetadata.errorCode() > 0) {
						throw new Exception("Failed to fetch topic metadata for the topic " + topic);
					}
					if (!topicMetadata.topic().equals(topic)) {
						continue;
					}
					tm = topicMetadata;
				}
			} catch (Exception e) {
				if (e instanceof ClosedChannelException) {
					throw new ConnectionException("Failure in communicating with Broker [" + ipAddress + ":" + port
							+ "] to find the Topic metadata of [" + topic + "].");
				}
				Thread.sleep(retryBackoffms);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		if (tm == null) {
			throw new MetadataUnavailableException("Please check if the topic " + topic + " exists already.");
		}
		return tm;
	}

	public static PartitionMetadata findNewLeader(final String topic, final int partitionID,
			final List<String> replicaBrokers, final String oldLeaderIp, final int oldLeaderPort,
			final HashSet<String> fullReplicaList, final long retryBackoffms) throws Exception {
		PartitionMetadata partitionMetadata = null;
		final List<String> replicas = new ArrayList<String>();
		replicas.addAll(replicaBrokers);
		if (replicas.size() == 1) {
			final String[] address = replicas.get(0).split(":");
			if (address[0].equals(oldLeaderIp) && Integer.parseInt(address[1]) == oldLeaderPort) {
				for (final String brokerid : fullReplicaList) {
					if (!replicas.contains(brokerid)) {
						replicas.add(brokerid);
					}
				}
			}
		}
		for (int i = 0; i < 2; ++i) {
			for (final String brokerAddress : replicas) {
				boolean retry = false;
				final String[] address2 = brokerAddress.split(":");
				try {
					final TopicMetadata topicMetadata = lookupTopicMetadata(address2[0], Integer.parseInt(address2[1]),
							topic, retryBackoffms);
					partitionMetadata = getPartitionMetadata(topicMetadata, partitionID);
					if (partitionMetadata == null) {
						retry = true;
					} else if (partitionMetadata.leader() == null || partitionMetadata.leader().host() == null
							|| partitionMetadata.leader().port() <= 0) {
						retry = true;
					} else {
						if (!oldLeaderIp.equalsIgnoreCase(partitionMetadata.leader().host())
								|| oldLeaderPort != partitionMetadata.leader().port() || i != 0) {
							return partitionMetadata;
						}
						if (KafkaUtils.logger.isDebugEnabled()) {
							KafkaUtils.logger.debug(
									(Object) ("Found the partition metadata. But the Old and new leader port and ip are same - in iteration - "
											+ i + ". So try again"));
						}
						retry = true;
					}
				} catch (Exception e) {
					if (e instanceof ClosedChannelException) {
						continue;
					}
				}
				if (retry) {
					try {
						Thread.sleep(retryBackoffms);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw ie;
					}
				}
			}
			for (final String brokerid : fullReplicaList) {
				if (!replicas.contains(brokerid)) {
					replicas.add(brokerid);
				}
			}
		}
		return partitionMetadata;
	}

	public static PartitionMetadata getPartitionMetadata(final TopicMetadata topicMetadata, final int partitionID) {
		for (final PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
			if (partitionMetadata.partitionId() == partitionID) {
				return partitionMetadata;
			}
		}
		return null;
	}

	static {
		logger = Logger.getLogger((Class) KafkaUtils.class);
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
