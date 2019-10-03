package com.datasphere.proc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.exc.ConnectionException;
import com.datasphere.common.exc.MetadataUnavailableException;
import com.datasphere.event.Event;
import com.datasphere.kafka.KafkaException;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.recovery.KafkaSourcePosition;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.source.kafka.KafkaPartitionHandler;
import com.datasphere.source.kafka.KafkaPartitionHandler_8;
import com.datasphere.source.kafka.KafkaProperty;
import com.datasphere.source.kafka.utils.KafkaUtils;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.type.positiontype;
import com.datasphere.uuid.UUID;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.message.MessageAndOffset;

@PropertyTemplate(name = "KafkaReader", version = "0.8.0", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "brokerAddress", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "blocksize", type = Integer.class, required = false, defaultValue = "10240"),
		@PropertyTemplateProperty(name = "Topic", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "PartitionIDList", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "startOffset", type = Integer.class, required = false, defaultValue = "-1"),
		@PropertyTemplateProperty(name = "KafkaConfig", type = String.class, required = false, defaultValue = "") }, inputType = HDEvent.class, requiresParser = true)
public class KafkaV8Reader extends SourceProcess {
	public static final String DISTRIBUTION_ID = "distributionId";
	private static final Logger logger;
	private String topic;
	private List<Integer> partitionIdList;
	private UUID sourceRef;
	private Map<String, Object> localCopyOfProperty;
	private boolean stopFetching;
	private int blocksize;
	private KafkaProperty prop;
	private PartitionedSourcePosition kafkaSourcePositions;
	private boolean sendPositions;
	private TopicMetadata topicMetaData;
	private positiontype posType;
	public Map<Integer, KafkaPartitionHandler> partitionMap;
	private int partitionCount;
	private String distributionId;
	private long retryBackoffms;
	private long leaderLookUp;
	private HashSet<String> replicaList;
	private ExecutorService threadPool;

	public KafkaV8Reader() {
		this.stopFetching = false;
		this.sendPositions = false;
		this.partitionCount = 0;
		this.distributionId = null;
		this.retryBackoffms = 1000L;
		this.leaderLookUp = 5L;
	}

	public void init(final Map<String, Object> prop1, final Map<String, Object> prop2, final UUID uuid,
			final String distributionId, final SourcePosition startPosition, final boolean sendPositions,
			final Flow flow) throws Exception {
		super.init((Map) prop1, (Map) prop2, uuid, distributionId);
		this.sourceRef = uuid;
		(this.localCopyOfProperty = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER)).putAll(prop1);
		this.localCopyOfProperty.putAll(prop2);
		this.localCopyOfProperty.put(BaseReader.SOURCE_PROCESS, this);
		this.localCopyOfProperty.put(Property.SOURCE_UUID, this.sourceRef);
		this.localCopyOfProperty.put("distributionId", distributionId);
		this.localCopyOfProperty.put("so_timeout", 30010);
		if (this.localCopyOfProperty.containsKey("startTimestamp")) {
			throw new RuntimeException(
					"Positioning by timestamp is only supported from Version 10. Version 8 supports only Position by offset. Supported values are 0(beginning of all partition(s)) or -1 (End of all partition(s)).");
		}
		this.kafkaSourcePositions = new PartitionedSourcePosition();
		this.prop = new KafkaProperty((Map) this.localCopyOfProperty);
		if (this.prop.positionValue > 0 || this.prop.positionValue < -1) {
			throw new RuntimeException(
					"Positioning by kafka read offset is not supported in Verison 8.Supported values are 0(beginning of all partition(s)) or -1 (End of all partition(s))");
		}
		this.topic = this.prop.topic;
		final String partitionList = this.prop.partitionList;
		this.blocksize = this.prop.blocksize * 1024;
		this.stopFetching = false;
		this.distributionId = distributionId;
		this.sendPositions = sendPositions;
		this.partitionCount = 0;
		final String[] kafkaConfig = this.prop.getKafkaBrokerConfigList();
		if (kafkaConfig != null) {
			for (int i = 0; i < kafkaConfig.length; ++i) {
				final String[] property = kafkaConfig[i].split("=");
				if (property == null || property.length < 2) {
					KafkaV8Reader.logger.warn((Object) ("Kafka Property \"" + property[0] + "\" is invalid."));
					KafkaV8Reader.logger.warn((Object) ("Invalid \"KafkaConfig\" property structure " + property[0]
							+ ". Expected structure <name>=<value>;<name>=<value>"));
				} else if (property[0].equals("retry.backoff.ms")) {
					this.retryBackoffms = Long.parseLong(property[1]);
				} else if (property[0].equals("retries")) {
					this.leaderLookUp = Long.parseLong(property[1]);
				} else if (property[0].equalsIgnoreCase("batch.size")) {
					final int batchSize = Integer.parseInt(property[1]);
					if (batchSize > this.blocksize) {
						this.blocksize = batchSize;
					}
				}
			}
		}
		boolean topicMetdaDataFound = false;
		if (this.prop.kafkaBrokerAddress != null) {
			this.replicaList = new HashSet<String>();
			for (int j = 0; j < this.prop.kafkaBrokerAddress.length; ++j) {
				this.replicaList.add(this.prop.kafkaBrokerAddress[j]);
			}
			for (int j = 0; j < this.prop.kafkaBrokerAddress.length && !topicMetdaDataFound; ++j) {
				try {
					final String[] ipAndPort = this.prop.kafkaBrokerAddress[j].split(":");
					final String brokerIpAddres = ipAndPort[0];
					final int brokerPortNo = Integer.parseInt(ipAndPort[1]);
					this.topicMetaData = KafkaUtils.lookupTopicMetadata(brokerIpAddres, brokerPortNo, this.topic,
							this.retryBackoffms);
					if (this.topicMetaData != null) {
						topicMetdaDataFound = true;
					}
				} catch (Exception e) {
					KafkaV8Reader.logger.error((Object) e);
					if (j == this.prop.kafkaBrokerAddress.length - 1) {
						throw new MetadataUnavailableException(
								"Failure in getting metadata for Kafka Topic. Closing the Kafka Reader " + e);
					}
				}
			}
		}
		if (KafkaV8Reader.logger.isDebugEnabled()) {
			KafkaV8Reader.logger.info((Object) this.topicMetaData);
		}
		if (partitionList != null && !partitionList.trim().isEmpty()) {
			this.partitionIdList = new ArrayList<Integer>();
			if (partitionList.contains(";")) {
				final String[] partitionIds = partitionList.split(";");
				for (int k = 0; k < partitionIds.length; ++k) {
					this.partitionIdList.add(Integer.parseInt(partitionIds[k]));
					++this.partitionCount;
				}
			} else {
				this.partitionIdList.add(Integer.parseInt(partitionList));
				++this.partitionCount;
			}
		} else {
			if (KafkaV8Reader.logger.isInfoEnabled()) {
				KafkaV8Reader.logger.info((Object) ("No partitions were specified for the topic - " + this.topic
						+ ". Trying to fetch partition list from Topic Metadata"));
			}
			this.partitionCount = this.topicMetaData.partitionsMetadata().size();
			this.partitionIdList = new ArrayList<Integer>();
			for (int j = 0; j < this.partitionCount; ++j) {
				final int partitionId = this.topicMetaData.partitionsMetadata().get(j).partitionId();
				this.partitionIdList.add(partitionId);
			}
			if (this.partitionIdList.isEmpty()) {
				throw new IllegalArgumentException("Unable to fetch the parition IDs for the topic - " + this.topic);
			}
		}
		if (this.sendPositions) {
			if (startPosition != null && startPosition instanceof PartitionedSourcePosition
					&& !((PartitionedSourcePosition) startPosition).isEmpty()) {
				this.kafkaSourcePositions = (PartitionedSourcePosition) startPosition;
				if (KafkaV8Reader.logger.isDebugEnabled()) {
					KafkaV8Reader.logger.info((Object) ("Restart position " + this.kafkaSourcePositions.toString()));
				}
				this.posType = positiontype.WA_POSITION_OFFSET;
			} else {
				this.posType = this.prop.posType;
			}
		} else {
			this.posType = this.prop.posType;
		}
		final ThreadFactory kafkaParserThreadFactory = new ThreadFactoryBuilder().setNameFormat("KafkaParserThread-%d")
				.build();
		this.threadPool = Executors.newCachedThreadPool(kafkaParserThreadFactory);
		this.initializeConsumer(this.topicMetaData);
		if (KafkaV8Reader.logger.isInfoEnabled()) {
			KafkaV8Reader.logger.info((Object) ("Data will be consumed from topic - " + this.topic + "(partitions - "
					+ this.partitionIdList.toString() + ")"));
		}
	}

	public String getTopic() {
		return this.topic;
	}

	public Position getCheckpoint() {
		if (this.sendPositions && !this.stopFetching) {
			final PathManager p = new PathManager();
			if (!this.partitionMap.isEmpty()) {
				for (final Map.Entry item : this.partitionMap.entrySet()) {
					final KafkaPartitionHandler metadata = (KafkaPartitionHandler) item.getValue();
					final KafkaSourcePosition sp = metadata.getCurrentPosition();
					if (sp != null) {
						p.mergeHigherPositions(
								Position.from(this.sourceUUID, metadata.getDistributionId(), (SourcePosition) sp));
					}
				}
				return p.toPosition();
			}
		}
		return null;
	}

	public void initializeConsumer(final TopicMetadata topicMetaData) throws Exception {
		int noOfInvalidParitionId = 0;
		this.partitionMap = new ConcurrentHashMap<Integer, KafkaPartitionHandler>();
		for (final Integer partitionId : this.partitionIdList) {
			boolean recovery = false;
			long kafkaReadOffset = 0L;
			KafkaSourcePosition sp = null;
			final PartitionMetadata partitionMetadata = KafkaUtils.getPartitionMetadata(topicMetaData, partitionId);
			if (partitionMetadata == null) {
				KafkaV8Reader.logger.warn(
						(Object) ("Partition with ID " + partitionId + " does not exist in the topic " + this.topic));
				++noOfInvalidParitionId;
			} else {
				if (this.posType == positiontype.WA_POSITION_OFFSET) {
					final String key = (this.distributionId != null && !this.distributionId.isEmpty())
							? (this.distributionId + ":" + this.topic + "-" + partitionId)
							: (this.topic + "-" + partitionId);
					sp = (KafkaSourcePosition) this.kafkaSourcePositions.get(key);
					if (sp != null) {
						kafkaReadOffset = sp.getKafkaReadOffset();
						recovery = true;
						if (KafkaV8Reader.logger.isInfoEnabled()) {
							KafkaV8Reader.logger.info((Object) ("Restart position available. Kafka Read offset is "
									+ kafkaReadOffset + " for " + this.topic + "-" + partitionId));
						}
					} else {
						kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId,
								kafka.api.OffsetRequest.EarliestTime(), partitionMetadata.leader().host(),
								partitionMetadata.leader().port());
						if (KafkaV8Reader.logger.isInfoEnabled()) {
							KafkaV8Reader.logger.info(
									(Object) ("Restart position was not found. Hence reading from beginning. Read offset is "
											+ kafkaReadOffset + " for " + this.topic + "-" + partitionId));
						}
					}
				} else if (this.posType == positiontype.WA_POSITION_EOF) {
					kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId,
							kafka.api.OffsetRequest.LatestTime(), partitionMetadata.leader().host(),
							partitionMetadata.leader().port());
					if (KafkaV8Reader.logger.isInfoEnabled()) {
						KafkaV8Reader.logger.info((Object) ("Restart by EOF (End of File). Read offset is "
								+ kafkaReadOffset + " for " + this.topic + "-" + partitionId));
					}
				} else if (this.posType == positiontype.WA_POSITION_SOF) {
					kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId,
							kafka.api.OffsetRequest.EarliestTime(), partitionMetadata.leader().host(),
							partitionMetadata.leader().port());
					if (KafkaV8Reader.logger.isInfoEnabled()) {
						KafkaV8Reader.logger.info((Object) ("Restart by SOF (Start of File). Read offset is "
								+ kafkaReadOffset + " for " + this.topic + "-" + partitionId));
					}
				}
				final KafkaProperty tmpProp = new KafkaProperty(this.prop.propMap);
				tmpProp.propMap.put("PartitionID", partitionId);
				tmpProp.propMap.put("KafkaReadOffset", kafkaReadOffset);
				tmpProp.propMap.put("fetchSize", this.blocksize);
				final KafkaPartitionHandler metadata = new KafkaPartitionHandler_8();
				((KafkaPartitionHandler_8) metadata).init(partitionMetadata, tmpProp);
				metadata.sendPosition(this.sendPositions);
				if (recovery) {
					metadata.setRestartPosition(sp);
				}
				this.replicaList.addAll(((KafkaPartitionHandler_8) metadata).replicaBrokers);
				this.partitionMap.put(partitionId, metadata);
				this.threadPool.submit((Runnable) metadata);
			}
		}
		if (this.partitionIdList.size() == noOfInvalidParitionId) {
			throw new MetadataUnavailableException(
					"Failure in Kafka Connection due to invalid Topic name or partition id. PartitionIDs "
							+ this.partitionIdList.toString() + "does not exist in the Topic " + this.topic
							+ ". Please check your topic name and number of partitions in it.");
		}
	}

	public void startConsumingData() throws Exception {
		final ByteBuffer payload = ByteBuffer
				.allocate(this.blocksize + Constant.INTEGER_SIZE + Constant.LONG_SIZE + Constant.BYTE_SIZE);
		int emptyPartitionCount = 0;
		try {
			if (this.partitionMap.isEmpty()) {
				this.stopFetching = true;
				KafkaV8Reader.logger.warn((Object) "No Kafka Brokers available to fetch data ");
				throw new ConnectionException(
						"Failure in connecting to Kafka broker. No Kafka Brokers available to fetch data.");
			}
			final Iterator<Entry<Integer, KafkaPartitionHandler>> it = this.partitionMap.entrySet().iterator();
			while (it.hasNext()) {
				final Map.Entry item = it.next();
				KafkaPartitionHandler metadata = (KafkaPartitionHandler) item.getValue();
				if (this.stopFetching) {
					if (((KafkaPartitionHandler_8) metadata).consumer != null) {
						((KafkaPartitionHandler_8) metadata).consumer.close();
						((KafkaPartitionHandler_8) metadata).consumer = null;
					}
					return;
				}
				if (((KafkaPartitionHandler_8) metadata).consumer == null) {
					((KafkaPartitionHandler_8) metadata).initializeConsumer();
				}
				final kafka.api.FetchRequest req = new FetchRequestBuilder().clientId(metadata.clientName)
						.addFetch(this.topic, metadata.partitionId,
								((KafkaPartitionHandler_8) metadata).kafkaReadOffset, this.blocksize)
						.maxWait(10).build();
				FetchResponse fetchResponse = null;
				try {
					fetchResponse = ((KafkaPartitionHandler_8) metadata).consumer.fetch(req);
				} catch (Exception exception) {
					if (this.stopFetching) {
						if (KafkaV8Reader.logger.isInfoEnabled()) {
							KafkaV8Reader.logger.info((Object) "Shutting down WebAction Kafka consumer");
						}
						break;
					}
					if (!(exception instanceof EOFException) && !(exception instanceof ClosedChannelException)) {
						continue;
					}
					if (((KafkaPartitionHandler_8) metadata).getLeaderLookUpCount() > this.leaderLookUp) {
						KafkaV8Reader.logger.warn((Object) ("Failure in fetching data from [" + this.topic + ","
								+ metadata.partitionId + "] since leader is not available."));
						it.remove();
						metadata.cleanUp();
						continue;
					}
					((KafkaPartitionHandler_8) metadata).increaseLeaderLookUpCount(1);
					KafkaV8Reader.logger.warn((Object) ("Leader (" + metadata.leaderIp + ":" + metadata.leaderPort
							+ ") is dead. Try to find the new Leader for [" + this.topic + "," + metadata.partitionId
							+ "]."));
					try {
						Thread.sleep(this.retryBackoffms);
						final PartitionMetadata partitionMetadata = KafkaUtils.findNewLeader(this.topic,
								metadata.partitionId, ((KafkaPartitionHandler_8) metadata).replicaBrokers,
								metadata.leaderIp, metadata.leaderPort, this.replicaList, this.retryBackoffms);
						final long newReadOffset = ((KafkaPartitionHandler_8) metadata).kafkaReadOffset;
						if (partitionMetadata != null) {
							((KafkaPartitionHandler_8) metadata).updatePartitionMetadata(partitionMetadata,
									newReadOffset);
							this.replicaList.addAll(((KafkaPartitionHandler_8) metadata).replicaBrokers);
							it.remove();
							this.partitionMap.put(metadata.partitionId, metadata);
							if (KafkaV8Reader.logger.isInfoEnabled()) {
								KafkaV8Reader.logger.info((Object) ("Consumer " + metadata.clientName
										+ " will start consuming data from (" + metadata.leaderIp + ":"
										+ metadata.leaderPort + ")for (" + this.topic + "-" + metadata.partitionId
										+ ") from Offset " + ((KafkaPartitionHandler_8) metadata).kafkaReadOffset
										+ "."));
							}
							break;
						}
						KafkaV8Reader.logger.warn((Object) ("Could not find a new Leader for [" + this.topic + ","
								+ metadata.partitionId + "]."));
					} catch (Exception e) {
						KafkaV8Reader.logger.warn((Object) ("Unable to find new Leader for [" + this.topic + ","
								+ metadata.partitionId + "]."));
						KafkaV8Reader.logger.warn((Object) e);
						metadata.cleanUp();
						it.remove();
					}
					continue;
				}
				if (fetchResponse.hasError()) {
					((KafkaPartitionHandler_8) metadata).increaseErrorCount(1);
					final short code = fetchResponse.errorCode(this.topic, metadata.partitionId);
					if (code == ErrorMapping.UnknownTopicOrPartitionCode()) {
						final Throwable cause = ErrorMapping.exceptionFor(code);
						final String message = cause.getMessage();
						KafkaV8Reader.logger.error((Object) ("Topic name is Invalid. " + message));
						throw new MetadataUnavailableException(this.topic + "Topic is Invalid.", cause);
					}
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						final Throwable cause = ErrorMapping.exceptionFor(code);
						final KafkaException k = new KafkaException(
								"Invalid read offset" + ((KafkaPartitionHandler_8) metadata).kafkaReadOffset, cause);
						KafkaV8Reader.logger.error((Object) (k.getCause() + " " + k.getMessage()));
						((KafkaPartitionHandler_8) metadata).kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic,
								metadata.partitionId, kafka.api.OffsetRequest.LatestTime(), metadata.leaderIp,
								metadata.leaderPort);
						if (!KafkaV8Reader.logger.isInfoEnabled()) {
							continue;
						}
						KafkaV8Reader.logger
								.info((Object) ("Updated the read Offset of [" + this.topic + "," + metadata.partitionId
										+ "] to " + ((KafkaPartitionHandler_8) metadata).kafkaReadOffset));
					} else if (code == ErrorMapping.NotLeaderForPartitionCode()) {
						if (KafkaV8Reader.logger.isInfoEnabled()) {
							KafkaV8Reader.logger.info((Object) ("Leader of [" + this.topic + "," + metadata.partitionId
									+ "] has changed. Trying to find the new Leader for [" + this.topic + ","
									+ metadata.partitionId + "]."));
						}
						if (((KafkaPartitionHandler_8) metadata).getLeaderLookUpCount() > this.leaderLookUp) {
							KafkaV8Reader.logger.warn((Object) ("Unable to fetch data from [" + this.topic + ","
									+ metadata.partitionId + "] since leader is not available."));
							it.remove();
							metadata.cleanUp();
						} else {
							((KafkaPartitionHandler_8) metadata).increaseLeaderLookUpCount(1);
							try {
								Thread.sleep(this.retryBackoffms);
								final PartitionMetadata partitionMetadata = KafkaUtils.findNewLeader(this.topic,
										metadata.partitionId, ((KafkaPartitionHandler_8) metadata).replicaBrokers,
										metadata.leaderIp, metadata.leaderPort, this.replicaList, this.retryBackoffms);
								final long newReadOffset = ((KafkaPartitionHandler_8) metadata).kafkaReadOffset;
								if (partitionMetadata != null) {
									((KafkaPartitionHandler_8) metadata).updatePartitionMetadata(partitionMetadata,
											newReadOffset);
									this.replicaList.addAll(((KafkaPartitionHandler_8) metadata).replicaBrokers);
									it.remove();
									this.partitionMap.put(metadata.partitionId, metadata);
									if (KafkaV8Reader.logger.isInfoEnabled()) {
										KafkaV8Reader.logger.info((Object) ("Consumer " + metadata.clientName
												+ " will start consuming data from (" + metadata.leaderIp + ":"
												+ metadata.leaderPort + ")for (" + this.topic + "-"
												+ metadata.partitionId + ") from Offset "
												+ ((KafkaPartitionHandler_8) metadata).kafkaReadOffset + "."));
									}
									break;
								}
								KafkaV8Reader.logger.warn((Object) ("Could not find a new Leader for [" + this.topic
										+ "," + metadata.partitionId + "]."));
							} catch (Exception e) {
								KafkaV8Reader.logger.warn((Object) ("Unable to find new Leader for [" + this.topic + ","
										+ metadata.partitionId + "]."));
								KafkaV8Reader.logger.warn((Object) e);
								metadata.cleanUp();
								it.remove();
							}
						}
					} else {
						if (((KafkaPartitionHandler_8) metadata).noOfErrors() <= 10) {
							continue;
						}
						final Throwable cause = ErrorMapping.exceptionFor(code);
						final KafkaException k = new KafkaException(
								"Fetching response for [" + this.topic + "," + metadata.partitionId + "] has a error ",
								cause);
						KafkaV8Reader.logger.error((Object) (k.getCause() + " " + k.getMessage()));
						KafkaV8Reader.logger.warn((Object) ("Stopped fetching data from [" + this.topic + ","
								+ metadata.partitionId + "]"));
						metadata.cleanUp();
						it.remove();
					}
				} else {
					if (fetchResponse.messageSet(this.topic, metadata.partitionId).sizeInBytes() > 0) {
						for (final MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.topic,
								metadata.partitionId)) {
							if (this.stopFetching) {
								return;
							}
							final long currentOffset = messageAndOffset.offset();
							if (currentOffset < ((KafkaPartitionHandler_8) metadata).kafkaReadOffset) {
								KafkaV8Reader.logger.warn((Object) ("Found an old offset: " + currentOffset
										+ " Expecting: " + ((KafkaPartitionHandler_8) metadata).kafkaReadOffset));
							} else {
								((KafkaPartitionHandler_8) metadata).kafkaReadOffset = messageAndOffset.nextOffset();
								if (messageAndOffset.message().payloadSize() <= 0) {
									continue;
								}
								payload.clear();
								final int messageSize = messageAndOffset.message().payloadSize();
								payload.putLong(currentOffset);
								payload.put((byte) 0);
								payload.putInt(messageSize);
								payload.put(messageAndOffset.message().payload());
								payload.flip();
								this.writeData(messageSize, payload, metadata);
							}
						}
					} else {
						++emptyPartitionCount;
					}
					metadata = null;
					if (emptyPartitionCount % this.partitionCount != 0) {
						continue;
					}
					Thread.sleep(10L);
					if (emptyPartitionCount != 5000) {
						continue;
					}
					KafkaV8Reader.logger.warn((Object) ("No messages were consumed from the topic '" + this.topic
							+ "' (partitions - " + this.partitionIdList
							+ ") for a while. Please check if the value of \"blockSize\" property specified as "
							+ this.blocksize
							+ " bytes is greater than \"max.message.bytes\" (in topic configuration) or \"message.max.bytes\" (in broker configuration file). Please ignore this warning if no message was produced to the Topic."));
				}
			}
		} catch (Exception e2) {
			if (e2 instanceof InterruptedException || e2 instanceof InterruptedIOException) {
				if (KafkaV8Reader.logger.isDebugEnabled()) {
					KafkaV8Reader.logger.info((Object) e2);
				}
			} else if (!this.stopFetching) {
				throw new ConnectionException((Throwable) e2);
			}
		}
	}

	public void writeData(final int messageSize, final ByteBuffer payload, final KafkaPartitionHandler metadata)
			throws IOException {
		final int totalMessageSize = messageSize + Constant.BYTE_SIZE + Constant.INTEGER_SIZE + Constant.LONG_SIZE;
		final byte[] bytes = new byte[totalMessageSize];
		payload.get(bytes, 0, totalMessageSize);
		if (metadata.getPipedOut() != null) {
			metadata.getPipedOut().write(bytes);
		}
	}

	public void receiveImpl(final int channel, final Event out) throws Exception {
		try {
			synchronized (this) {
				if (!this.stopFetching) {
					this.startConsumingData();
				}
			}
		} catch (Exception e) {
			KafkaV8Reader.logger.error((Object) e);
			throw new ConnectionException("Failure in Kafka Connection. Closing the KafkaReader. ", (Throwable) e);
		}
	}

	public void close() throws Exception {
		this.stopFetching = true;
		if (this.partitionMap != null && !this.partitionMap.isEmpty()) {
			final Iterator it = this.partitionMap.entrySet().iterator();
			while (it.hasNext()) {
				((KafkaPartitionHandler) ((Map.Entry) it.next()).getValue()).stopFetching(true);
			}
			while (it.hasNext()) {
				try {
					((KafkaPartitionHandler) ((Map.Entry) it.next()).getValue()).cleanUp();
					it.remove();
				} catch (Exception e) {
					KafkaV8Reader.logger.warn((Object) ("Problem while stopping KafkaReader " + e.getMessage()));
				}
			}
		}
		synchronized (this) {
			if (this.threadPool != null) {
				this.threadPool.shutdown();
				try {
					if (!this.threadPool.awaitTermination(2000L, TimeUnit.MILLISECONDS)) {
						this.threadPool.shutdownNow();
					}
				} catch (InterruptedException e2) {
					if (KafkaV8Reader.logger.isInfoEnabled()) {
						KafkaV8Reader.logger.info((Object) ("Parser thread interrupted to terminate forcefully " + e2));
					}
				}
				this.threadPool = null;
			}
		}
	}

	public boolean requiresPartitionedSourcePosition() {
		return true;
	}

	public static void main(final String[] args) {
		final HashMap<String, Object> mr = new HashMap<String, Object>();
		mr.put("brokerAddress", "localhost:9092");
		mr.put("startOffset", 0);
		mr.put("Topic", "dsv_5p");
		final HashMap<String, Object> mp = new HashMap<String, Object>();
		mp.put("handler", "DSVParser");
		final KafkaV8Reader kf = new KafkaV8Reader();
		System.out.println("Gonna initialize");
		final UUID uuid = new UUID(System.currentTimeMillis());
		mp.put(Property.SOURCE_UUID, uuid);
		try {
			kf.init(mr, mp, uuid, null, null, true, null);
			kf.receiveImpl(0, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static {
		logger = Logger.getLogger((Class) KafkaV8Reader.class);
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
