package com.datasphere.runtime.monitor;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.datasphere.event.SimpleEvent;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datasphere.tungsten.CluiMonitorView;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.HD;

public class MonitorEvent extends SimpleEvent implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 7197475144117417143L;
    public static final String SERVER_ID = "serverID";
    public static final String ENTITY_ID = "entityID";
    public static final String VALUE_LONG = "valueLong";
    public static final String VALUE_STRING = "valueString";
    public static final String TIME_STAMP = "timeStamp";
    public static final String TYPE = "type";
    private static Logger logger;
    public static final UUID typeID;
    public static final UUID RollupUUID;
    public static final String MONITORING_SCHEMA = "{  \"context\": [     { \"name\": \"serverID\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"entityID\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"valueLong\", \"type\": \"long\", \"nullable\": true , \"index\": \"false\" },    { \"name\": \"valueString\", \"type\": \"string\", \"nullable\": true , \"index\": \"false\" },    { \"name\": \"timeStamp\", \"type\": \"long\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"type\", \"type\": \"string\", \"nullable\": false , \"index\": \"false\" }  ]}";
    public static final String ORDERBY_CLAUSE = " , \"orderby\": [    { \"attr\": \"timeStamp\", \"ascending\": %s }  ]";
    public static final Type[] KAFKA_TYPES;
    public static final Type[] ES_TYPES;
    public long id;
    public UUID serverID;
    public UUID entityID;
    public Type type;
    public Long valueLong;
    public String valueString;
    public Operation operation;
    
    public static String getOrderByClause(final String isAscending) {
        return String.format(" , \"orderby\": [    { \"attr\": \"timeStamp\", \"ascending\": %s }  ]", isAscending);
    }
    
    public MonitorEvent() {
        this.operation = Operation.NONE;
    }
    
    public MonitorEvent(final MonitorEvent other) {
        this.operation = Operation.NONE;
        this.timeStamp = other.timeStamp;
        this._da_SimpleEvent_ID = new UUID();
        this.serverID = other.serverID;
        this.entityID = other.entityID;
        this.type = other.type;
        this.valueLong = other.valueLong;
        this.valueString = other.valueString;
        this.operation = other.operation;
    }
    
    public MonitorEvent(final long timestamp) {
        super(timestamp);
        this.operation = Operation.NONE;
        this.serverID = null;
        this.entityID = null;
        this.type = null;
        this.valueLong = null;
        this.timeStamp = -1L;
    }
    
    public MonitorEvent(final UUID serverID, final UUID entityID, final Type type, final Long valueLong, final Long timeStamp) {
        super(System.currentTimeMillis());
        this.operation = Operation.NONE;
        if (serverID == null) {
            throw new RuntimeException("serverID cannot be null");
        }
        if (entityID == null) {
            throw new RuntimeException("entityID cannot be null");
        }
        this.serverID = serverID;
        this.entityID = entityID;
        this.type = type;
        this.valueLong = valueLong;
        this.timeStamp = timeStamp;
    }
    
    public MonitorEvent(final UUID serverID, final UUID componentID, final Type type, final String valueString, final Long timeStamp) {
        super(System.currentTimeMillis());
        this.operation = Operation.NONE;
        this.serverID = serverID;
        this.entityID = componentID;
        this.type = type;
        this.valueString = valueString;
        this.timeStamp = timeStamp;
        if (serverID == null) {
            throw new RuntimeException("serverID cannot be null");
        }
        if (this.entityID == null) {
            throw new RuntimeException("entityID cannot be null");
        }
    }
    
    public MonitorEvent(final HD hd) {
        super(System.currentTimeMillis());
        this.operation = Operation.NONE;
        this.serverID = (hd.get("serverID").isNull() ? null : new UUID(hd.get("serverID").asText()));
        this.entityID = (hd.get("entityID").isNull() ? null : new UUID(hd.get("entityID").asText()));
        this.type = (hd.get("type").isNull() ? null : Type.valueOf(hd.get("type").asText()));
        this.valueString = (hd.get("valueString").isNull() ? null : hd.get("valueString").asText());
        this.valueLong = (hd.get("valueLong").isNull() ? null : hd.get("valueLong").asLong());
        this.timeStamp = (hd.get("timeStamp").isNull() ? null : hd.get("timeStamp").asLong());
        if (this.serverID == null) {
            throw new RuntimeException("serverID cannot be null");
        }
        if (this.entityID == null) {
            throw new RuntimeException("entityID cannot be null");
        }
    }
    
    public MonitorEvent(final UUID serverID, final UUID entityID, final Type type, final Long value, final long timeStamp, final Operation operation) {
        this.operation = Operation.NONE;
        if (serverID == null) {
            throw new RuntimeException("serverID cannot be null");
        }
        if (entityID == null) {
            throw new RuntimeException("entityID cannot be null");
        }
        this.serverID = serverID;
        this.entityID = entityID;
        this.type = type;
        this.valueLong = value;
        this.timeStamp = timeStamp;
        this.operation = operation;
    }
    
    public MonitorEvent(final UUID serverID, final UUID entityID, final Type type, final String value, final long timeStamp, final Operation operation) {
        this.operation = Operation.NONE;
        if (serverID == null) {
            throw new RuntimeException("serverID cannot be null");
        }
        if (entityID == null) {
            throw new RuntimeException("entityID cannot be null");
        }
        this.serverID = serverID;
        this.entityID = entityID;
        this.type = type;
        this.valueString = value;
        this.timeStamp = timeStamp;
        this.operation = operation;
    }
    
    public String toString() {
        final Date d = new Date(this.timeStamp);
        final String timeStr = CluiMonitorView.DATE_FORMAT_MILLIS.format(d);
        final String serverIDString = MonitorEvent.RollupUUID.equals((Object)this.serverID) ? "<ROLLUP>" : this.serverID.getUUIDString();
        return "MonitorEvent { serverID:" + serverIDString + ", entityID:" + this.entityID + ", timestamp:" + timeStr + " (" + this.timeStamp + ")}, type:" + this.type + ", value:" + ((this.valueString != null) ? this.valueString : (this.valueLong + ", operaton: " + this.operation));
    }
    
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MonitorEvent)) {
            return false;
        }
        final MonitorEvent that = (MonitorEvent)obj;
        final boolean a = this.id == that.id;
        final boolean b = (this.serverID == null) ? (that.serverID == null) : this.serverID.equals((Object)that.serverID);
        final boolean c = (this.entityID == null) ? (that.entityID == null) : this.entityID.equals((Object)that.entityID);
        final boolean d = (this.type == null) ? (that.type == null) : this.type.equals(that.type);
        final boolean e = (this.valueLong == null) ? (that.valueLong == null) : this.valueLong.equals(that.valueLong);
        final boolean f = (this.valueString == null) ? (that.valueString == null) : this.valueString.equals(that.valueString);
        final boolean g = this.timeStamp == that.timeStamp;
        return a && b && c && d && e && f && g;
    }
    
    public int hashCode() {
        return new HashCodeBuilder().append(this.id).append((Object)this.serverID).append((Object)this.entityID).append(this.type.hashCode()).append((Object)this.valueLong).append((Object)this.valueString).append(this.timeStamp).toHashCode();
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeLong(this.id);
        kryo.writeObjectOrNull(output, (Object)this.serverID, (Class)UUID.class);
        kryo.writeObjectOrNull(output, (Object)this.entityID, (Class)UUID.class);
        output.writeString(this.type.name());
        kryo.writeObjectOrNull(output, (Object)this.valueLong, (Class)Long.class);
        output.writeString(this.valueString);
        output.writeString(this.operation.name());
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.id = input.readLong();
        this.serverID = (UUID)kryo.readObjectOrNull(input, (Class)UUID.class);
        this.entityID = (UUID)kryo.readObjectOrNull(input, (Class)UUID.class);
        this.type = Type.valueOf(input.readString());
        this.valueLong = (Long)kryo.readObjectOrNull(input, (Class)Long.class);
        this.valueString = input.readString();
        this.operation = Operation.valueOf(input.readString());
    }
    
    public UUID getServerID() {
        return this.serverID;
    }
    
    public UUID getEntityID() {
        return this.entityID;
    }
    
    public HD gethd() {
        final HD hd = new HD();
        hd.put("serverID", (this.serverID != null) ? this.serverID.toString() : null);
        hd.put("entityID", (this.entityID != null) ? this.entityID.toString() : null);
        hd.put("valueLong", this.valueLong);
        hd.put("valueString", this.valueString);
        hd.put("timeStamp", this.timeStamp);
        hd.put("type", this.type.name());
        return hd;
    }
    
    public static String getFields() {
        return "*";
    }
    
    public static String getWhereClauseJson(final Map<String, Object> params) {
        if (params.isEmpty()) {
            return "";
        }
        final StringBuilder result = new StringBuilder();
        result.append(",  \"where\": {     \"and\": [");
        final Iterator<String> entries = params.keySet().iterator();
        while (entries.hasNext()) {
            final String entry = entries.next();
            if (entry.equalsIgnoreCase("serverID")) {
                result.append("    { \"oper\": \"eq\",");
                result.append("      \"attr\": \"serverID\",");
                result.append("      \"value\": \"").append(params.get(entry)).append("\" }");
            }
            if (entry.equalsIgnoreCase("entityID")) {
                result.append("    { \"oper\": \"eq\",");
                result.append("      \"attr\": \"entityID\",");
                result.append("      \"value\": \"").append(params.get(entry)).append("\" }");
            }
            if (entry.equalsIgnoreCase("startTime")) {
                result.append("    { \"oper\": \"gte\",");
                result.append("      \"attr\": \"timeStamp\",");
                result.append("      \"value\": ").append(params.get(entry)).append(" }");
            }
            if (entry.equalsIgnoreCase("datumNames")) {
                result.append("    { \"oper\": \"in\",");
                result.append("      \"attr\": \"type\",");
                result.append("      \"value\": \"").append(params.get(entry)).append("\" }");
            }
            if (entry.equalsIgnoreCase("endTime")) {
                result.append("    { \"oper\": \"lte\",");
                result.append("      \"attr\": \"timeStamp\",");
                result.append("      \"value\": ").append(params.get(entry)).append(" }");
            }
            if (entries.hasNext()) {
                result.append(",");
            }
        }
        result.append("    ]  }");
        return result.toString();
    }
    
    static {
        MonitorEvent.logger = Logger.getLogger((Class)MonitorEvent.class);
        typeID = new UUID("7b1f8438-947c-4d10-9fe3-ac8f4a34af27");
        RollupUUID = new UUID("8c209549-058d-5e21-a0f4-bd905b45b038");
        KAFKA_TYPES = new Type[] { Type.KAFKA_BROKERS, Type.KAFKA_BYTES_RATE, Type.KAFKA_MSGS_RATE, Type.LATEST_ACTIVITY };
        ES_TYPES = new Type[] { Type.ES_FREE_BYTES, Type.ES_RX_BYTES, Type.ES_TOTAL_BYTES, Type.ES_TX_BYTES, Type.ES_HOT_THREADS };
    }
    
    public enum Type
    {
        ADAPTER_BASIC("Adapters brief event type"), 
        ADAPTER_DETAIL("Adapters detailed event type"), 
        CACHE_REFRESH("Cache Refresh time"), 
        CACHE_SIZE("Number of events held in Cache"), 
        CHECKPOINT_SUMMARY("A text summary of the checkpoint for this component"), 
        CLUSTER_NAME("Name of cluster"), 
        COMMENTS("Any useful notes about a component"), 
        CORES("Number of cores"), 
        CPU_PER_NODE("Number of cpus per node"), 
        CPU_RATE("CPU rate at nanos per second"), 
        CPU_THREAD("CPU time of thread"), 
        CURRENT_FILENAME("Name of the file that is being currently read from/written to"), 
        DISK_FREE("summary of free space on all local disks"), 
        ELASTICSEARCH_FREE("free space available to elasticsearch"), 
        ES_DOCS("Elasticsearch docs"), 
        ES_FETCHES("Elasticsearch fetches"), 
        ES_FREE_BYTES("Free Bytes in Elasticsearch"), 
        ES_HOT_THREADS("Description of Elasticsearch hot threads"), 
        ES_TOTAL_BYTES("Total Elasticsearch Bytes"), 
        ES_TX_BYTES("Elasticsearch Transacion bytes"), 
        ES_QUERIES("Elasticsearch queries"), 
        ES_RX_BYTES("Elasticsearch RX bytes"), 
        ES_SIZE_BYTES("Elasticsearch Bytes size"), 
        ES_QUERY_RATE("Elasticsearch Query rate"), 
        ES_FETCH_RATE("Elasticsearch Fetch rate"), 
        ES_DOCS_RATE("Elasticsearch Docs rate"), 
        ES_SIZE_BYTES_RATE("Elasticsearch bytes rate"), 
        EOF_STATUS("Indicates whether the current file being read has reached EOF or not"), 
        INPUT("Total number of events input into a component since it started"), 
        INPUT_RATE("Recent rate of events input into a component"), 
        KAFKA_BROKERS("List of Kafka brokers"), 
        KAFKA_BYTES("Number of Kafka bytes"), 
        KAFKA_BYTES_RATE("Kafka bytes rate"), 
        KAFKA_BYTES_RATE_LONG("Kafka bytes rate long"), 
        KAFKA_MSGS("Number of Kafka messages"), 
        KAFKA_MSGS_RATE("Kafka messages rate"), 
        KAFKA_MSGS_RATE_LONG("Kafka messages rate long"), 
        LATEST_ACTIVITY("Time of last known activity"), 
        LOCAL_HITS("Total Cache Lookup hits"), 
        LOCAL_HITS_RATE("Local hits rate"), 
        LOCAL_MISSES("Total Cache Lookup misses"), 
        LOCAL_MISSES_RATE("Local miss rate"), 
        LOG_ERROR("Log error"), 
        LOG_WARN("Log warning"), 
        LOOKUPS("Number of Lookups"), 
        LOOKUPS_RATE("Recent rate of Cache lookups"), 
        LAST_COMMIT_TIME("Last commit executed time"), 
        LAST_IO_TIME("Last IO Time"), 
        LAST_UPLOADED_FILENAME("Name of the last uploaded file"), 
        LAST_CHECKPOINTED_POSITION("Value of last checkpointed position"), 
        LAST_ROLLEDOVER_FILENAME("Name of the last rolled over file that was written"), 
        LAST_ROLLEDOVER_TIME("Time at which last written file was rolled over"), 
        LAST_FILE_READ("Name of the file that was read before moving to a new file"), 
        LAST_FILE_READ_TIME("Time at which last file was read before moving ot a new file"), 
        LAST_READ_FILE_OFFSET("Last read offset of the current file being read from the beginning"), 
        MAC_ADDRESS("MAC address of physical hardware"), 
        EXTERNAL_IO_LATENCY("External IO Latency"), 
        TOTAL_EVENTS_IN_LAST_IO("Total events in last IO"), 
        TOTAL_EVENTS_IN_LAST_COMMIT("Total events in last commit"), 
        TOTAL_EVENTS_IN_LAST_UPLOAD("Total number of events in last upload"), 
        TOTAL_EVENTS_IN_CURRENT_FILE("Total number of events written in current file"), 
        TOTAL_EVENTS_IN_PREVIOUS_FILE("Totla number of events written in previous rolled over file"), 
        MEMORY_FREE("Free memory"), 
        MEMORY_MAX("Maximum memory"), 
        MEMORY_TOTAL("Total Memory"), 
        NUM_LOG_ERRORS("Number of Log Errors"), 
        NUM_LOG_WARNS("Number of Log warnings"), 
        NUM_PARTITIONS("Number of partitions"), 
        NUM_OF_EXCEPTIONS_IGNORED("Number of exceptions ignored"), 
        OUTPUT("Total events output from a component"), 
        OUTPUT_RATE("Recent rate of events output from a component"), 
        PROCESSED("Total events processed by a component since it started"), 
        PROCESSED_RATE("Processed Rate"), 
        RANGE_HEAD("Range Head"), 
        RANGE_TAIL("Range Tail"), 
        RATE("Rate"), 
        RECEIVED("Received"), 
        RECEIVED_RATE("Received Rate"), 
        REMOTE_HITS("Number of Remote hits"), 
        REMOTE_HITS_RATE("Remote hits rate"), 
        REMOTE_MISSES("Number of Remote misses"), 
        REMOTE_MISSES_RATE("Remote miss rate"), 
        SOURCE_INPUT("Source Input"), 
        SOURCE_RATE("Source rate"), 
        STATUS_CHANGE("Change in status"), 
        STREAM_FULL("If stream is full"), 
        TARGET_ACKED("Number of events acked by an adapter to a Target"), 
        TARGET_RATE("Target rate"), 
        TARGET_OUTPUT("Target Output"), 
        SENT_BYTES_RATE_MB_per_SEC("Sent Bytes Rate"), 
        WRITE_BYTES_RATE_MB_per_SEC("Write Bytes Rate"), 
        UPTIME("Up time"), 
        VERSION("Version"), 
        HDS_CREATED("Number of hds created"), 
        HDS_CREATED_RATE("Creation date of hds"), 
        WINDOW_SIZE("Size of window"), 
        LAST_IO_SIZE("Size of the last upload in bytes"), 
        ORA_READER_LAST_OBSERVED_SCN("Last observed LCR-SCN"), 
        ORA_READER_LAST_OBSERVED_TIMESTAMP("Last observed LCR-Timestamp"), 
        START_SCN("StartSCN"), 
        LAG_REPORT("Lag Record"), 
        LAG_RATE("Lag Rate"), 
        LAG_REPORT_JSON("Lag Rate JSON Formatted"), 
        TARGET_COMMIT_POSITION("Commit Position"), 
        LAST_CHECKPOINT_POSITION("Last Checkpoint position"), 
        CDC_OPERATION("CDC Operations"), 
        TOTAL_SELECTS("Total Number of Selects"), 
        READ_LAG("Read Lag"), 
        AVG_LATENCY("Avg Latency between Kafka Send and Ack"), 
        MAX_LATENCY("Max Latency between Kafka Send and Ack"), 
        AVG_TIME_BETWEEN_TWO_SEND_CALLS("Avg time between two kafka send calls"), 
        MAX_TIME_BETWEEN_TWO_SEND_CALLS("Max time between two kafka send calls"), 
        KAFKA_CONSUMER_RATE_MB_per_SEC("Read bytes rate"), 
        KAFKA_CONSUMER_MSG_RATE("Messages read per sec"), 
        TOTAL_KAFKA_MSGS_READ("Total messages read from a topic"), 
        TOTAL_KAFKA_SEND_CALLS("Total number of kafka send calls"), 
        TOTAL_KINESIS_SEND_CALLS("Total number of Kinesis send calls"), 
        LAST_OBSERVED_POSITION("Last observed audit trail position"), 
        LAST_OBSERVED_TIMESTAMP("Last observed audit trail timestamp"), 
        KAFKA_READER_PARTITION_DISTRIBUTION_LIST("List of partitions"), 
        PACKETS_PROCESSED("Packets Processed"), 
        PACKETS_DISCARDGED("Packets Disacard"), 
        BYTES_PROCESSES("Bytes Processed"), 
        BYTES_DISCARDED("Bytes Discarded"), 
        NO_OF_OPEN_TXN("No of Open Transactions"), 
        OPERATION_METRICS("Individual Operation Count"), 
        CATALOG_EVOLUTION_STATUS("Schema Evolution Status"), 
        DISCARDED_EVENT_COUNT("Discarded Event Count"), 
        READ_STATUS("Read Status");
        
        private String description;
        
        private Type(final String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return this.description;
        }
    }
    
    public enum Operation
    {
        SUM, 
        AVG, 
        MIN, 
        MAX, 
        NONE;
    }
}
