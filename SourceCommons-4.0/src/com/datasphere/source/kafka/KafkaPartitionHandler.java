package com.datasphere.source.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.classloading.ParserLoader;
import com.datasphere.common.exc.RecordException;
import com.datasphere.event.Event;
import com.datasphere.intf.Parser;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.BaseReader;
import com.datasphere.proc.SourceProcess;
import com.datasphere.recovery.KafkaSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.source.lib.intf.CharParser;
import com.datasphere.source.lib.io.common.EventPipedInputStream;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;

public class KafkaPartitionHandler implements Runnable
{
    public String clientName;
    public long recordSeqNo;
    public Offset kafkaMsgOffset;
    public int partitionId;
    public String leaderIp;
    public int leaderPort;
    protected String topic;
    protected int blocksize;
    private Reader reader;
    private EventPipedInputStream eventPipedIn;
    private PipedOutputStream pipedOut;
    private Parser parser;
    private volatile boolean stopFetching;
    private boolean sendPosition;
    private SourceProcess sourceProcess;
    private UUID sourceRef;
    private String distributionId;
    private String nodeId;
    private KafkaSourcePosition waitPosition;
    private KafkaSourcePosition currentPosition;
    private static final long MAX_SLEEP_TIME = 10L;
    private static final Logger logger;
    
    public KafkaPartitionHandler() {
        this.recordSeqNo = 0L;
        this.kafkaMsgOffset = null;
        this.blocksize = 0;
        this.parser = null;
        this.stopFetching = false;
        this.waitPosition = null;
        this.currentPosition = null;
    }
    
    public PipedOutputStream getPipedOut() {
        return this.pipedOut;
    }
    
    public void init(final Property prop, final EventPipedInputStream inputStream) {
        this.eventPipedIn = inputStream;
        this.init(prop);
    }
    
    public void init(final Property prop) {
        this.blocksize = Integer.parseInt((String)prop.propMap.get("fetchSize"));
        this.pipedOut = new PipedOutputStream();
        if (this.eventPipedIn == null) {
            this.eventPipedIn = new OLVPipedInputStream(this.blocksize * 2);
        }
        this.sourceProcess = (SourceProcess)prop.propMap.get(BaseReader.SOURCE_PROCESS);
        this.sourceRef = (UUID)prop.propMap.get(Property.SOURCE_UUID);
        this.nodeId = (String)prop.propMap.get("distributionId");
        this.topic = (String)prop.propMap.get("Topic");
        this.partitionId = Integer.parseInt((String)prop.propMap.get("PartitionID"));
        this.distributionId = this.topic + "-" + this.partitionId;
        prop.propMap.put(Reader.STREAM, this.eventPipedIn);
        prop.propMap.put(Reader.READER_TYPE, Reader.KAFKA_READER);
        try {
            this.eventPipedIn.connect(this.pipedOut);
            this.parser = ParserLoader.loadParser((Map)prop.propMap, this.sourceRef);
            if (!(this.parser instanceof CharParser) && prop.getMap().containsKey(Property.CHARSET)) {
                prop.getMap().remove(Property.CHARSET);
            }
            this.reader = Reader.createInstance(prop);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void cleanUp() throws Exception {
        this.stopFetching = true;
        if (this.eventPipedIn != null) {
            this.eventPipedIn.close();
            this.eventPipedIn = null;
        }
        if (this.pipedOut != null) {
            this.pipedOut.close();
            this.pipedOut = null;
        }
        if (this.parser != null) {
            this.parser.close();
            this.parser = null;
        }
        if (KafkaPartitionHandler.logger.isInfoEnabled()) {
            KafkaPartitionHandler.logger.info((Object)("Closed Parser Thread # " + Thread.currentThread().getId() + " of [" + this.topic + "," + this.partitionId + "]."));
        }
    }
    
    public Reader getReader() {
        return this.reader;
    }
    
    public boolean isStopFetching() {
        return this.stopFetching;
    }
    
    public void stopFetching(final boolean stop) {
        this.stopFetching = stop;
    }
    
    public void sendPosition(final boolean sendPosition) {
        this.sendPosition = sendPosition;
    }
    
    public int getBlocksize() {
        return this.blocksize;
    }
    
    public void setRestartPosition(final KafkaSourcePosition sp) {
        this.waitPosition = sp;
        this.currentPosition = sp;
    }
    
    public KafkaSourcePosition getCurrentPosition() {
        return this.currentPosition;
    }
    
    public String getDistributionId() {
        return this.distributionId;
    }
    
    @Override
    public void run() {
        try {
            if (KafkaPartitionHandler.logger.isInfoEnabled()) {
                KafkaPartitionHandler.logger.info((Object)("(Thread # " + Thread.currentThread().getId() + ") Started the HD Kafka Consumer for [" + this.topic + "," + this.partitionId + "] from " + this.leaderIp + ":" + this.leaderPort + "."));
            }
            final Iterator<Event> eventItr = (Iterator<Event>)this.parser.parse((InputStream)this.reader);
            while (!this.stopFetching && !Thread.currentThread().isInterrupted()) {
                try {
                    while (eventItr.hasNext() && !this.stopFetching && !Thread.currentThread().isInterrupted()) {
                        final Event event = eventItr.next();
                        Position outPos = null;
                        if (this.sendPosition) {
                            if (this.kafkaMsgOffset == null || this.kafkaMsgOffset.compareTo(this.eventPipedIn.getMessageOffset()) < 0) {
                                this.recordSeqNo = 0L;
                                this.kafkaMsgOffset = this.eventPipedIn.getMessageOffset();
                            }
                            else {
                                ++this.recordSeqNo;
                            }
                            if (this.waitPosition != null) {
                                if (this.kafkaMsgOffset.compareTo((Offset)new KafkaLongOffset(this.waitPosition.getKafkaReadOffset())) == 0 && this.recordSeqNo <= this.waitPosition.getRecordSeqNo()) {
                                    if (KafkaPartitionHandler.logger.isDebugEnabled()) {
                                        KafkaPartitionHandler.logger.debug((Object)("Skipping the event " + event + " with Kafka Offset-record Sequence No " + this.kafkaMsgOffset + "-" + this.recordSeqNo));
                                        continue;
                                    }
                                    continue;
                                }
                                else {
                                    this.waitPosition = null;
                                }
                            }
                            final KafkaSourcePosition sp = new KafkaSourcePosition(this.topic, this.partitionId, this.recordSeqNo, (long)this.kafkaMsgOffset.getOffset());
                            outPos = Position.from(this.sourceRef, this.distributionId, (SourcePosition)sp);
                            this.sourceProcess.send(event, 0, outPos);
                            this.currentPosition = sp;
                        }
                        else {
                            this.sourceProcess.send(event, 0, outPos);
                        }
                    }
                    if (this.stopFetching) {
                        continue;
                    }
                    Thread.sleep(10L);
                    continue;
                }
                catch (RuntimeException exp) {
                    final Throwable t = exp.getCause();
                    if (t instanceof RecordException) {
                        if (t.getCause() instanceof IOException) {
                            if (this.eventPipedIn != null) {
                                if (KafkaPartitionHandler.logger.isInfoEnabled()) {
                                    KafkaPartitionHandler.logger.info((Object)exp);
                                }
                                else if (!this.stopFetching) {
                                    KafkaPartitionHandler.logger.warn((Object)exp);
                                }
                            }
                            break;
                        }
                        if (((RecordException)t).type() == RecordException.Type.INVALID_RECORD) {
                            if (Thread.currentThread().isInterrupted()) {
                                continue;
                            }
                            KafkaPartitionHandler.logger.warn((Object)exp.getMessage());
                            continue;
                        }
                        if (((RecordException)t).type() == RecordException.Type.END_OF_DATASOURCE) {
                            break;
                        }
                    }
                    throw exp;
                }
                catch (Exception exception) {
                    throw exception;
                }
            }
        }
        catch (Exception exception2) {
            if (exception2 instanceof InterruptedException || exception2 instanceof InterruptedIOException) {
                if (KafkaPartitionHandler.logger.isDebugEnabled()) {
                    KafkaPartitionHandler.logger.debug((Object)("Exception caused by stopping application " + exception2));
                }
            }
            else if (!this.stopFetching) {
                KafkaPartitionHandler.logger.warn((Object)exception2);
            }
            try {
                this.cleanUp();
            }
            catch (Exception e) {
                if (KafkaPartitionHandler.logger.isTraceEnabled()) {
                    KafkaPartitionHandler.logger.trace((Object)("Excepition caused during cleanUp. This can be ignored " + e));
                }
            }
        }
        finally {
            try {
                this.cleanUp();
            }
            catch (Exception e2) {
                if (KafkaPartitionHandler.logger.isTraceEnabled()) {
                    KafkaPartitionHandler.logger.trace((Object)("Excepition caused during cleanUp. This can be ignored " + e2));
                }
            }
        }
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaPartitionHandler.class);
    }
}
