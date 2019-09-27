package com.datasphere.proc;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

import org.apache.log4j.Logger;

import com.datasphere.classloading.ParserLoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.InvalidDataException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.event.Event;
import com.datasphere.intf.Analyzer;
import com.datasphere.intf.Parser;
import com.datasphere.intf.SourceMetadataProvider;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.MonitorableComponent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.security.WASecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.recovery.BaseReaderSourcePosition;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.source.lib.intf.CharParser;
import com.datasphere.source.lib.intf.CheckpointProvider;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;

public class BaseReader extends SourceProcess implements Analyzer
{
    Logger logger;
    public final String PARSER_NAME = "handler";
    public final String BINARY_PARSER_NAME = "BinaryParser";
    public final String XML_PARSER = "XMLParser";
    public final String JSON_PARSER = "JSONParser";
    public final String ISBANK_PARSER = "IsBankParser";
    public final String GG_PARSER = "GGTrailParser";
    public final String SNMP_PARSER = "SNMPParser";
    public final String COLLECTD_PARSER = "CollectdParser";
    public final String VALID_RECORD = "VALID_RECORD";
    public final String INVALID_RECORD = "INVALID_RECORD";
    public static String SOURCE_PROCESS;
    private Parser parser;
    protected boolean streamType;
    private long lastByteCount;
    static final String TABLE_NAME = "Tablename";
    private boolean sendPositions;
    SourcePosition sourcePosition;
    int noRecordCount;
    int recordCount;
    int sleepTime;
    int maxSleepTime;
    boolean breakOnNoRecord;
    boolean closeCalled;
    protected String readerType;
    protected CheckpointDetail recordCheckpoint;
    boolean supportCheckpoint;
    boolean isAnalyzer;
    Iterator<Event> iterator;
    private boolean readerParserInitialized;
    private Map<String, Object> localPropertyMap;
    private InputStream reader;
    protected String componentName;
    protected MonitorableComponent monitorableComponent;
    boolean testModeLoop;
    Map<String, Object> prop1Save;
    Map<String, Object> prop2Save;
    UUID uuidSave;
    String distributionIdSave;
    SourcePosition startPositionSave;
    boolean sendPositionsSave;
    Flow flowSave;
    
    public BaseReader() {
        this.logger = Logger.getLogger((Class)BaseReader.class);
        this.lastByteCount = 0L;
        this.sendPositions = false;
        this.noRecordCount = 0;
        this.recordCount = 0;
        this.sleepTime = 0;
        this.maxSleepTime = 100;
        this.readerType = "";
        this.recordCheckpoint = null;
        this.readerParserInitialized = false;
        this.componentName = "";
        this.testModeLoop = false;
    }
    
    void retryWait() {
        if (this.sleepTime < 10000) {
            ++this.sleepTime;
        }
        else if (this.sleepTime < 20000) {
            Thread.yield();
            ++this.sleepTime;
        }
        else {
            LockSupport.parkNanos(this.sleepTime);
            if (this.sleepTime < this.maxSleepTime * 1000000) {
                this.sleepTime += 10000;
            }
        }
    }
    
    public synchronized void receiveImpl(final int channel, final Event out) throws Exception {
        if (!this.readerParserInitialized) {
            this.parser = this.createParser(this.localPropertyMap);
            if (!(this.parser instanceof CharParser)) {
                this.localPropertyMap.remove(Property.CHARSET);
            }
            this.breakOnNoRecord = new Property(this.localPropertyMap).getBoolean("breakonnorecord", false);
            if (this.recordCheckpoint != null) {
                this.reader = this.createInputStream(this.localPropertyMap, true);
            }
            else {
                this.reader = this.createInputStream(this.localPropertyMap);
            }
            if (this.reader != null && this.reader instanceof Reader) {
                this.maxSleepTime = ((Reader)this.reader).eofdelay();
            }
            if (this.reader != null) {
                ((Reader)this.reader).setComponentName(this.componentName);
                ((Reader)this.reader).setComponentUUID(this.sourceUUID);
                String distributionID;
                if (this.distributionID != null) {
                    distributionID = this.distributionID;
                }
                else {
                    distributionID = HazelcastSingleton.getBindingInterface();
                }
                ((Reader)this.reader).setDistributionID(distributionID);
            }
            if (this.reader != null && this.reader instanceof Reader) {
                if (this.localPropertyMap.get("handler").equals("XMLParser")) {
                    final Reader tmp = (Reader)this.reader;
                    this.reader = Reader.XMLPositioner(tmp);
                }
                else if (this.localPropertyMap.get("handler").equals("GGTrailParser")) {
                    final Reader tmp = (Reader)this.reader;
                    this.reader = Reader.GGTrailPositioner(tmp);
                }
                ((Reader)this.reader).position(this.recordCheckpoint, true);
            }
            this.iterator = (Iterator<Event>)this.parser.parse(this.reader);
            if (this.parser instanceof CheckpointProvider) {
                this.supportCheckpoint = true;
            }
            if (this.parser instanceof Observer && this.reader instanceof Reader) {
                ((Reader)this.reader).registerObserver(this.parser);
            }
            if (this.parser instanceof Analyzer) {
                this.isAnalyzer = true;
            }
            if (this.parser instanceof MonitorableComponent) {
                this.monitorableComponent = (MonitorableComponent)this.parser;
            }
//            else if (this.reader instanceof MonitorableComponent) {
//                this.monitorableComponent = (MonitorableComponent)this.reader;
//            }
            if (this.parser == null || (!this.isAnalyzer && this.iterator == null)) {
                throw new AdapterException("Parser is not instantiated as the handler passed is null");
            }
            this.readerParserInitialized = true;
        }
        else {
            try {
                if (!this.iterator.hasNext()) {
                    if (this.breakOnNoRecord) {
                        throw new RecordException(RecordException.Type.NO_RECORD);
                    }
                    if (this.testModeLoop) {
                        this.close();
                        this.init(this.prop1Save, this.prop2Save, this.uuidSave, this.distributionIdSave, this.startPositionSave, this.sendPositionsSave, this.flowSave);
                        return;
                    }
                    this.retryWait();
                }
                else {
                    Position outPos = null;
                    final Event event = this.iterator.next();
                    if (this.supportCheckpoint) {
                        final CheckpointDetail recordCheckpoint = ((CheckpointProvider)this.parser).getCheckpointDetail();
                        final long thisByteCount = recordCheckpoint.getBytesRead();
                        final long sourceBytes = thisByteCount - this.lastByteCount;
                        this.lastByteCount = thisByteCount;
                        final BaseReaderSourcePosition sourcePosition = (recordCheckpoint != null) ? new BaseReaderSourcePosition(recordCheckpoint) : null;
                        outPos = (this.sendPositions ? Position.from(this.sourceUUID, this.distributionID, (SourcePosition)sourcePosition) : null);
                        this.sourcePosition = (SourcePosition)sourcePosition;
                        this.metricsCollector.setSourcePosition((SourcePosition)sourcePosition, this.sourceUUID);
                        this.metricsCollector.addSourceBytes(sourceBytes, this.sourceUUID);
                    }
                    if (this.logger.isTraceEnabled()) {
                        this.logger.trace((Object)("Sending event from BaseReader : " + event.toString()));
                    }
                    this.send(event, 0, outPos);
                    ++this.recordCount;
                    this.sleepTime = 0;
                }
            }
            catch (RecordException recordExp) {
                if (recordExp.type() == RecordException.Type.NO_RECORD) {
                    if (this.breakOnNoRecord) {
                        throw recordExp;
                    }
                    if (this.testModeLoop) {
                        this.close();
                        this.init(this.prop1Save, this.prop2Save, this.uuidSave, this.distributionIdSave, this.startPositionSave, this.sendPositionsSave, this.flowSave);
                        return;
                    }
                    this.retryWait();
                }
                else if (recordExp.type() != RecordException.Type.INVALID_RECORD) {
                    if (!this.closeCalled) {
                        throw recordExp;
                    }
                }
            }
            catch (RuntimeException runtimeExp) {
                final Throwable t = runtimeExp.getCause();
                if (t instanceof RecordException && !this.closeCalled) {
                    final RecordException rExp = (RecordException)t;
                    rExp.errMsg(runtimeExp.getMessage());
                    throw rExp;
                }
                if (t instanceof InvalidDataException && !this.closeCalled) {
                    final InvalidDataException ide = (InvalidDataException)t;
                    throw ide;
                }
                if (!this.closeCalled) {
                    throw runtimeExp;
                }
            }
            catch (Exception exp) {
                exp.printStackTrace();
                throw exp;
            }
        }
    }
    
    public void init(final Map<String, Object> properties) throws Exception {
    }
    
    public void init(final Map<String, Object> prop1, final Map<String, Object> prop2, final UUID uuid, final String distributionId, final SourcePosition startPosition, final boolean sendPositions, final Flow flow) throws Exception {
        super.init((Map)prop1, (Map)prop2, uuid, distributionId, startPosition, sendPositions, flow);
        this.closeCalled = false;
        this.sendPositions = sendPositions;
        if (startPosition != null) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Source Position : [" + startPosition + "]"));
            }
        }
        else if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Null Source Position");
        }
        (this.localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER)).putAll(prop2);
        this.localPropertyMap.putAll(prop1);
        this.customizePropertyMap(this.localPropertyMap);
        this.localPropertyMap.put("distributionId", distributionId);
        this.localPropertyMap.put("restartPosition", startPosition);
        this.localPropertyMap.put("sendPositions", sendPositions);
        this.testModeLoop = new Property(this.localPropertyMap).getBoolean("testModeLoop", false);
        if (this.testModeLoop) {
            this.prop1Save = prop1;
            this.prop2Save = prop2;
            this.uuidSave = uuid;
            this.distributionIdSave = distributionId;
            this.startPositionSave = startPosition;
            this.sendPositionsSave = sendPositions;
            this.flowSave = flow;
        }
        if (startPosition != null && startPosition instanceof BaseReaderSourcePosition) {
            this.recordCheckpoint = ((BaseReaderSourcePosition)startPosition).recordCheckpoint;
        }
        else if (this.logger.isDebugEnabled() && startPosition != null) {
            this.logger.debug((Object)("Not using startPosition...[" + startPosition + "]"));
        }
        if (this.sourceUUID != null) {
            this.componentName = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, WASecurityManager.TOKEN).getFullName();
        }
    }
    
    public void close() throws Exception {
        this.closeCalled = true;
        this.readerParserInitialized = false;
        if (this.parser != null) {
            this.parser.close();
        }
    }
    
    public InputStream createInputStream(final Map<String, Object> prop) throws Exception {
        return this.createInputStream(prop, false);
    }
    
    public InputStream createInputStream(final Map<String, Object> prop, final boolean recoveryMode) throws Exception {
        if (!this.readerType.isEmpty()) {
            prop.put(Property.READER_TYPE, this.readerType);
        }
        return Reader.createInstance(null, new Property(prop), recoveryMode);
    }
    
    public Parser createParser(final Map<String, Object> prop) throws Exception {
        return ParserLoader.loadParser((Map)prop, this.sourceUUID);
    }
    
    public Map<String, TypeDefOrName> getMetadata() throws Exception {
        if (this.parser instanceof SourceMetadataProvider) {
            return (Map<String, TypeDefOrName>)((SourceMetadataProvider)this.parser).getMetadata();
        }
        return null;
    }
    
    public String getMetadataKey() {
        if (this.parser instanceof SourceMetadataProvider) {
            return ((SourceMetadataProvider)this.parser).getMetadataKey();
        }
        return null;
    }
    
    public Position getCheckpoint() {
        Position result = (this.sourceUUID != null && this.sourcePosition != null) ? (result = new Position(new Path(this.sourceUUID, this.distributionID, this.sourcePosition))) : null;
        return result;
    }
    
    protected void customizePropertyMap(final Map<String, Object> prop) {
    }
    
    public Map<String, Object> getFileDetails() {
        if (this.isAnalyzer) {
            return (Map<String, Object>)((Analyzer)this.parser).getFileDetails();
        }
        return null;
    }
    
    public List<Map<String, Object>> getProbableProperties() {
        if (this.isAnalyzer) {
            return (List<Map<String, Object>>)((Analyzer)this.parser).getProbableProperties();
        }
        return null;
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        super.publishMonitorEvents(events);
//        if (this.monitorableComponent != null) {
//            this.monitorableComponent.publishMonitorEvents(events);
//        }
    }
    
    static {
        BaseReader.SOURCE_PROCESS = "sourceProcess";
    }
}
