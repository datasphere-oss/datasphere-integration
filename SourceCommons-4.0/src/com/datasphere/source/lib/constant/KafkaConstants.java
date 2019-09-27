package com.datasphere.source.lib.constant;

public class KafkaConstants
{
    public static final int MAX_NUMBER_OF_OFFSETS = 1;
    public static final String READ_OFFSET = "readOffset";
    public static final int MAX_NO_OF_ERROR = 10;
    public static final int MAX_LEADER_LOOKUP = 5;
    public static final String TOPIC_NAME = "topic_name";
    public static final String PARTITION_ID = "partition_id";
    public static final int MAX_FETCH_WAIT_TIME = 10;
    public static final int SO_TIMEOUT = 30010;
    public static final int METADATA_REQUEST_BUFFER_SIZE = 65536;
    public static final long POLL_TIMEOUT = 10000L;
    public static final int RETRIES = 0;
    public static final long RETRY_BACKOFF_MS = 2000L;
    public static final String KAFKA_MESSAGE_VERSION = "KafkaMessageFormatVersion";
    public static final String MESSAGE_VERSION_1 = "v1";
    public static final String MESSAGE_VERSION_2 = "v2";
    public static final String PRODUCER_MODE = "Mode";
    public static final String SYNC_MODE = "Sync";
    public static final String ASYNC_MODE = "Async";
    public static final String BATCHED_SYNC_MODE = "BatchedSync";
    public static final String POLL_TIMEOUT_CONFIG = "poll.timeout.ms";
    public static final int DEFAULT_BATCH_SIZE = 1000000;
    public static final long DEFAULT_BATCH_TIMEOUT = 200L;
    public static final int DEFAULT_MAX_FETCH_SIZE = 10485760;
    public static final int DEFAULT_MIN_FETCH_SIZE = 1048576;
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 1000;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 2000000;
    public static final String LATEST_POSITION = "latest_position";
    public static final String INITIAL_POSITION = "initial_position";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 60001;
    public static final int DEFAULT_SESSION_TIMEOUT_MS = 60000;
}
