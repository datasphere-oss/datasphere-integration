package com.datasphere.source.lib.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.intf.MonitorableComponent;
import com.datasphere.source.lib.intf.EventMetadataProvider;
import com.datasphere.source.lib.prop.Property;

public class Reader extends InputStream implements EventMetadataProvider, MonitorableComponent
{
    public static String READER_TYPE;
    public static String FILE_READER;
    public static String MULTI_FILE_READER;
    public static String UDP_READER;
    public static String TCP_READER;
    public static String JMS_READER;
    public static String STREAM_READER;
    public static String STREAM;
    public static String STRING_READER;
    public static String ENCODER;
    public static String DECOMPRESSOR;
    public static String ENRICHER;
    public static String ARCHIVE_EXTRACTOR;
    public static String THREADED_QUEUE;
    public static String KAFKA_READER;
    public static String RECOVERY_MODE;
    public static String RECOVERY_POSITIONER;
    public static String SOURCE_IP;
    public static String SOURCE_PORT;
    public static String DELETE_AFTER_USE;
    private Logger logger;
    private ReaderBase strategy;
    private long bytesRead;
    private boolean readerClosed;
    public final int DEFAULT_BLOCK_SIZE = 4096;
    
    public Reader(final ReaderBase strategy) {
        this.logger = Logger.getLogger((Class)Reader.class);
        this.bytesRead = 0L;
        this.readerClosed = false;
        this.strategy = strategy;
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("Reader is initialized with the following strategy " + strategy.name));
        }
    }
    
    public String name() {
        return this.strategy.name();
    }
    
    public Object readBlock() throws AdapterException {
        return this.strategy.readBlock();
    }
    
    public Object readBock(final boolean multiEndpointSupport) throws AdapterException {
        return this.strategy.readBlock(multiEndpointSupport);
    }
    
    public int blockSize() {
        return this.strategy.blockSize();
    }
    
    public void blockSize(final int block_size) {
        this.strategy.blockSize(block_size);
    }
    
    @Override
    public void close() throws IOException {
        if (!this.readerClosed) {
            this.strategy.close();
            this.readerClosed = true;
        }
    }
    
    public boolean isDone() {
        return false;
    }
    
    @Override
    public int read() throws IOException {
        return this.strategy.read();
    }
    
    @Override
    public int available() throws IOException {
        return this.strategy.available();
    }
    
    public CheckpointDetail getCheckpointDetail() {
        return this.strategy.getCheckpointDetail();
    }
    
    public long charactersToSkip() {
        return this.strategy.bytesToSkip();
    }
    
    @Override
    public long skip(final long characterOffset) throws IOException {
        return this.strategy.skip(characterOffset);
    }
    
    @Override
    public int read(final byte[] buffer, final int off, final int len) {
        final int bytesRead = this.strategy.read(buffer, off, len);
        if (bytesRead > 0) {
            this.bytesRead += bytesRead;
            this.strategy.recoveryCheckpoint.setBytesRead(this.bytesRead);
        }
        return bytesRead;
    }
    
    public byte[] peek(final int peekSize) throws AdapterException {
        return this.strategy.peek(peekSize);
    }
    
    public void discardPacket() throws AdapterException {
        this.strategy.discardPacket();
    }
    
    public long skipBytes(final long offset) throws AdapterException {
        return this.strategy.skipBytes(offset);
    }
    
    public void position(final CheckpointDetail record, final boolean position) throws AdapterException {
        this.strategy.position(record, position);
    }
    
    public int eofdelay() {
        return this.strategy.eofdelay();
    }
    
    public ReaderBase strategy() {
        return this.strategy;
    }
    
    public boolean supportsMutipleEndpoint() {
        return this.strategy.supportsMutipleEndpoint();
    }
    
    public void registerObserver(final Object observer) {
        this.strategy.registerObserver(observer);
    }
    
    public static Reader TCPReader(final Property prop) throws AdapterException {
        final ReaderBase tcpReader = new TCPReader(prop);
        tcpReader.init();
        return new Reader(tcpReader);
    }
    
    public static Reader TCPBufferedReader(final Property prop) throws AdapterException {
        final ReaderBase tcpBufferedReader = new QueueReader(new TCPReader(prop));
        tcpBufferedReader.init();
        return new Reader(tcpBufferedReader);
    }
    
    public static Reader UDPReader(final Property prop) throws AdapterException {
        final ReaderBase udpReader = new UDPReader(prop);
        udpReader.init();
        return new Reader(udpReader);
    }
    
    public static Reader UDPBufferedReader(final Property prop) throws AdapterException {
        final ReaderBase udpBufferedReader = new QueueReader(new UDPReader(prop));
        udpBufferedReader.init();
        return new Reader(udpBufferedReader);
    }
    
    public static Reader CharUDPReader(final Property prop) throws AdapterException {
        final ReaderBase charUDPReader = new Encoding(new UDPReader(prop));
        charUDPReader.init();
        return new Reader(charUDPReader);
    }
    
    public static Reader JMSReader(final Property prop) throws AdapterException {
        final ReaderBase jmsReader = new JMSReader(prop);
        jmsReader.init();
        return new Reader(jmsReader);
    }
    
    public static Reader CharJMSReader(final Property prop) throws AdapterException {
        final ReaderBase charJMSReader = new Encoding(new JMSReader(prop));
        charJMSReader.init();
        return new Reader(charJMSReader);
    }
    
    public static Reader UDPThreadedReader(final Property prop) throws AdapterException {
        final ReaderBase udpThreadedReader = new ThreadedQueueReader(new UDPReader(prop));
        udpThreadedReader.init();
        return new Reader(udpThreadedReader);
    }
    
    public static Reader CharUDPThreadedReader(final Property prop) throws AdapterException {
        final ReaderBase charUDPThreadedReader = new Encoding(new ThreadedQueueReader(new UDPReader(prop)));
        charUDPThreadedReader.init();
        return new Reader(charUDPThreadedReader);
    }
    
    public static Reader CharTCPBufferedReader(final Property prop) throws AdapterException {
        return new Reader(new CharBufferManager(new Encoding(new ThreadedQueueReader(new TCPReader(prop)))));
    }
    
    public static Reader TCPThreadedReader(final Property prop) throws AdapterException {
        final ReaderBase tcpThreadedReader = new ThreadedQueueReader(new TCPReader(prop));
        tcpThreadedReader.init();
        return new Reader(tcpThreadedReader);
    }
    
    public static Reader CharTCPThreadedReader(final Property prop) throws AdapterException {
        final ReaderBase charTCPThreadedReader = new Encoding(new ThreadedQueueReader(new TCPReader(prop)));
        charTCPThreadedReader.init();
        return new Reader(charTCPThreadedReader);
    }
    
    public static Reader FileReader(final Property prop) throws AdapterException {
        final ReaderBase fileReader = new FileReader(prop);
        fileReader.init();
        return new Reader(fileReader);
    }
    
    public static Reader CharFileReader(final Property prop) throws AdapterException {
        final ReaderBase charFileReader = new Encoding(new FileReader(prop));
        charFileReader.init();
        return new Reader(charFileReader);
    }
    
    public static Reader FileBufferedReader(final Property prop) throws AdapterException {
        final ReaderBase fileBufferedReader = new QueueReader(new FileReader(prop));
        fileBufferedReader.init();
        return new Reader(fileBufferedReader);
    }
    
    public static Reader XMLPositioner(final Reader reader) throws AdapterException {
        return new Reader(new XMLPositioner(reader.strategy()));
    }
    
    public static Reader GGTrailPositioner(final Reader reader) throws AdapterException {
        return new Reader(new GGTrailPositioner(reader.strategy()));
    }
    
    public static Reader addStrategy(final Reader reader, final String strategyType) throws AdapterException {
        final ReaderBase tmp = createInstance(strategyType, null, null);
        tmp.init(reader.strategy().property());
        return addStrategy(reader, tmp);
    }
    
    public static Reader addStrategy(final Reader reader, final ReaderBase anexture) throws AdapterException {
        anexture.downstream(reader.strategy());
        reader.strategy().upstream(anexture);
        return new Reader(anexture);
    }
    
    public static Reader createInstance(final Property prop) throws AdapterException {
        return createInstance(null, prop, false);
    }
    
    public static Reader createInstance(final ReaderBase readerBase, final Property prop, final boolean recoveryMode) throws AdapterException {
        if (recoveryMode) {
            prop.getMap().put(Reader.RECOVERY_MODE, true);
        }
        else {
            prop.getMap().put(Reader.RECOVERY_MODE, false);
        }
        ReaderBase reader;
        if (readerBase != null) {
            reader = readerBase;
        }
        else {
            reader = createInstance(prop.getString(Property.READER_TYPE, null), prop, null);
        }
        final String compressionType = prop.getString(Property.COMPRESSION_TYPE, null);
        reader = createInstance((compressionType != null && !compressionType.isEmpty()) ? Reader.DECOMPRESSOR : null, prop, reader);
        reader = createInstance((prop.getString(Property.ARCHIVE_TYPE, null) != null) ? Reader.ARCHIVE_EXTRACTOR : null, prop, reader);
        reader = createInstance(prop.getBoolean("skipbom", true) ? Reader.ENRICHER : null, null, reader);
        reader = createInstance((prop.getString(Property.CHARSET, null) != null) ? Reader.ENCODER : Reader.RECOVERY_POSITIONER, null, reader);
        reader.init();
        return new Reader(reader);
    }
    
    public static ReaderBase createInstance(final String type, final Property prop, final ReaderBase link) throws AdapterException {
        if (type == null) {
            return link;
        }
        if (type.equals(Reader.FILE_READER)) {
            if (prop.getBoolean(Property.NETWORK_FILE_SYSTEM, false)) {
                return new NFSReader(prop);
            }
            return new FileReader(prop);
        }
        else {
            if (type.equals(Reader.JMS_READER)) {
                return createInstance(Reader.THREADED_QUEUE, prop, new JMSReader(prop));
            }
            if (type.equals(Reader.TCP_READER)) {
                return createInstance(Reader.THREADED_QUEUE, prop, new TCPReader(prop));
            }
            if (type.equals(Reader.UDP_READER)) {
                return createInstance(Reader.THREADED_QUEUE, prop, new UDPReader(prop));
            }
            if (type.equals(Reader.KAFKA_READER)) {
                return new KafkaPartitionReader(prop, (InputStream)prop.getMap().get(Reader.STREAM));
            }
            if (type.equals(Reader.STREAM_READER)) {
                return new StreamReader(prop, (InputStream)prop.getMap().get(Reader.STREAM));
            }
            if (type.equals(Reader.ENCODER)) {
                return new Encoding(link);
            }
            if (type.equals(Reader.RECOVERY_POSITIONER)) {
                return new RecoveryPositioner(link);
            }
            if (type.equals(Reader.ENRICHER)) {
                return new Enricher(link);
            }
            if (type.equals(Reader.DECOMPRESSOR)) {
                return new Decompressor(link);
            }
            if (type.equals(Reader.ARCHIVE_EXTRACTOR)) {
                return new ArchiveExtractor(link);
            }
            if (type.equals(Reader.THREADED_QUEUE)) {
                return new ThreadedQueueReader(link);
            }
            if (type.equals(Reader.STRING_READER)) {
                return new StringReader(prop);
            }
            throw new AdapterException("No object is mapped for [" + type + "]");
        }
    }
    
    public static InputStream convertStringToInputStream(final String data, String charset) throws AdapterException {
        if (data == null || data.isEmpty()) {
            throw new AdapterException("String to be converted cannot be null or empty");
        }
        charset = ((charset != null) ? charset : "UTF-8");
        try {
            final InputStream is = new ByteArrayInputStream(data.getBytes(charset));
            return is;
        }
        catch (UnsupportedEncodingException e) {
            throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME, (Throwable)e);
        }
    }
    
    public void setInputStream(final InputStream dataSource) throws AdapterException {
        this.strategy.setInputStream(dataSource);
    }
    
    public void setCharacterBuffer(final CharBuffer buffer) {
        this.strategy.setCharacterBuffer(buffer);
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        return this.strategy.getEventMetadata();
    }
    
    @Override
    public String toString() {
        return this.name();
    }
    
    @Override
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        this.strategy.publishMonitorEvents(events);
    }
    
    public void setComponentName(final String componentName) {
        this.strategy.setComponentName(componentName);
    }
    
    public void setComponentUUID(final UUID componentUUID) {
        this.strategy.setComponentUUID(componentUUID);
    }
    
    public void setDistributionID(final String distributionID) {
        this.strategy.setDistributionID(distributionID);
    }
    
    public FileMetadataExtension getFileMetadataExtension() {
        return this.strategy.getFileMetadataExtension();
    }
    
    static {
        Reader.READER_TYPE = Property.READER_TYPE;
        Reader.FILE_READER = "FileReader";
        Reader.MULTI_FILE_READER = "MultiFileReader";
        Reader.UDP_READER = "UDPReader";
        Reader.TCP_READER = "TCPReader";
        Reader.JMS_READER = "JMSReader";
        Reader.STREAM_READER = "StreamReader";
        Reader.STREAM = "Stream";
        Reader.STRING_READER = "StringReader";
        Reader.ENCODER = "Encoder";
        Reader.DECOMPRESSOR = "Decompressor";
        Reader.ENRICHER = "Enricher";
        Reader.ARCHIVE_EXTRACTOR = "ArchiveExtractor";
        Reader.THREADED_QUEUE = "ThreadedQueueReader";
        Reader.KAFKA_READER = "KafkaReader";
        Reader.RECOVERY_MODE = "RecoveryMode";
        Reader.RECOVERY_POSITIONER = "RecoveryPositioner";
        Reader.SOURCE_IP = "SourceIP";
        Reader.SOURCE_PORT = "SourcePort";
        Reader.DELETE_AFTER_USE = "DeleteAfterUse";
    }
}
