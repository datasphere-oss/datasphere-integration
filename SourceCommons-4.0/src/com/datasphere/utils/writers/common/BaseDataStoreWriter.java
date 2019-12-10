package com.datasphere.utils.writers.common;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.datasphere.classloading.WALoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.BaseProcess;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.recovery.Acknowledgeable;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;

public abstract class BaseDataStoreWriter extends BaseProcess implements Acknowledgeable
{
    private static Logger logger;
    protected long eventCounter;
    protected Map<String, ArrayList<EventDataObject>> targetDataMap;
    private Timer batchTimer;
    private Exception lastException;
    protected WriterProperties writerProps;
    protected Position currentReceivedPosition;
    private final Object lockObject;
    protected String prevTargetTable;
    protected Map<UUID, Field[]> typeUUIDCache;
    protected Map<UUID, List<String>> typeUUIDKeyCache;
    
    public BaseDataStoreWriter() {
        this.eventCounter = 0L;
        this.targetDataMap = new HashMap<String, ArrayList<EventDataObject>>();
        this.lastException = null;
        this.currentReceivedPosition = null;
        this.lockObject = new Object();
        this.typeUUIDCache = new ConcurrentHashMap<UUID, Field[]>();
        this.typeUUIDKeyCache = new ConcurrentHashMap<UUID, List<String>>();
    }
    
    public final void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID streamUUID, final String did, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow) throws Exception {
        this.eventCounter = 0L;
        final TreeMap<String, Object> props = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        props.putAll(properties);
        if (streamUUID != null) {
            this.getTypeInfoFromStream(streamUUID);
        }
        props.put("streamUUID", streamUUID);
        this.writerProps = this.initWriter(props);
        if (this.writerProps == null) {
            throw new AdapterException("Writer Properties are not initialized properly");
        }
        final Iterator<String> iter = this.writerProps.getSourceTargetMap().values().iterator();
        while (iter.hasNext()) {
            this.targetDataMap.put(iter.next(), new ArrayList<EventDataObject>());
        }
        if (this.writerProps.getBatchInterval() > 0L) {
            this.initializeBatchTimer();
        }
        if (BaseDataStoreWriter.logger.isDebugEnabled()) {
            BaseDataStoreWriter.logger.debug((Object)("DataStore Writer initialized with " + this.writerProps));
        }
    }
    
    public final void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID) throws Exception {
        this.init(properties, properties2, sourceUUID, distributionID, null, false, null);
    }
    
    public final void init(final Map<String, Object> properties, final Map<String, Object> properties2) throws Exception {
        this.init(properties, properties2, null, null, null, false, null);
    }
    
    public final void init(final Map<String, Object> properties) throws Exception {
        this.init(properties, new HashMap<String, Object>(), null, null, null, false, null);
    }
    
    public final void receive(final int channel, final Event event, final Position pos) throws Exception {
        synchronized (this.lockObject) {
            if (pos != null) {
                this.currentReceivedPosition = pos;
            }
            this.receive(channel, event);
        }
    }
    
    public final void receiveImpl(final int channel, final Event event) throws Exception {
        if (this.lastException != null) {
            throw this.lastException;
        }
        this.process(event);
    }
    
    private void process(final Event event) throws Exception {
        HDEvent event;
        if (this.typedEvent) {
            event = this.convertTypedeventToHDevent(event);
        }
        else {
            event = (HDEvent)event;
        }
        this.updateAndExpireBatch(event);
    }
    //------暂时用以下替代  jack
    private HDEvent convertTypedeventToHDevent(Event event) {
    		HDEvent hdEvent = null;
		if (event instanceof HDEvent) {
			HDEvent evt = (HDEvent) event;
			if (evt.typeUUID == null) {
				evt.typeUUID = this.typeUUID;
			}
			return evt;
		} else {
			try {
				JSONObject object = new JSONObject(event.toString());
				if (object.isNull("data")) {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						hdEvent = new HDEvent(payloadLength, (UUID) null);
						hdEvent.typeUUID = this.typeUUID;
						hdEvent.data = new Object[payloadLength];
						int i = 0;
						for (final Object o : payload) {
							hdEvent.setData(i++, o);
						}
					}
				} else {
					final Object[] payload = event.getPayload();
					if (payload != null) {
						final int payloadLength = payload.length;
						hdEvent = new HDEvent(payloadLength, (UUID) null);
						hdEvent.typeUUID = this.typeUUID;
						hdEvent.data = new Object[payloadLength];
						int i = 0;
						for (final Object o : payload) {
							hdEvent.setData(i++, o);
						}
					} else {
						JSONArray objs = (JSONArray) object.get("data");
						int payloadLength = objs.length();
						hdEvent = new HDEvent(payloadLength, (UUID) null);
						hdEvent.typeUUID = this.typeUUID;
						hdEvent.data = new Object[payloadLength];
						for (int i = 0; i < objs.length(); i++) {
							Object obj = (Object) objs.get(i);
							hdEvent.setData(i, obj);
						}
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
		return hdEvent;
    }
    
    protected void updateAndExpireBatch(final HDEvent event) throws Exception {
        if (!event.metadata.containsKey("OperationName")) {
            throw new AdapterException("Invalid HDEvent format. " + this.getClass().getSimpleName() + " supports HDEvent originating from CDC and database sources only. For other sources, please send the typed event stream.");
        }
        if (!event.metadata.containsKey("TableName")) {
            BaseDataStoreWriter.logger.warn((Object)("Ignoring event: " + event + " a field " + "TableName" + " is required in metadata"));
            return;
        }
        final String sourceTableName = event.metadata.get("TableName").toString();
        final String isPKupdate = (String)event.metadata.get("PK_UPDATE");
        if (isPKupdate != null && isPKupdate.trim().equalsIgnoreCase("true")) {
            this.handlePKUpdateEvents(event, sourceTableName);
            return;
        }
        final EventDataObject edo = this.getEventDataObject(event, event.data);
        if (this.getSourceTargetMap().containsKey(sourceTableName)) {
            this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo);
        }
        else if (this.getSourceTargetMap().containsKey("*")) {
            this.targetDataMap.get(this.getSourceTargetMap().get("*")).add(edo);
        }
        else {
            BaseDataStoreWriter.logger.warn((Object)("Ignoring event " + event + " No Mapping found, source not mapped with appropriate target"));
        }
        ++this.eventCounter;
        if (this.eventCounter >= this.writerProps.getBatchCount()) {
            this.flushAll();
            this.eventCounter = 0L;
        }
    }
    
    protected EventDataObject getEventDataObject(final HDEvent event, final Object[] dataOrBeforeArray) throws AdapterException, MetaDataRepositoryException {
        Field[] fieldsOfThisTable = null;
        List<String> keys = null;
        if (event.typeUUID != null) {
            if (this.typeUUIDCache.containsKey(event.typeUUID) && this.typeUUIDKeyCache.containsKey(event.typeUUID)) {
                fieldsOfThisTable = this.typeUUIDCache.get(event.typeUUID);
                keys = this.typeUUIDKeyCache.get(event.typeUUID);
            }
            else {
                try {
                    final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, HDSecurityManager.TOKEN);
                    keys = dataType.keyFields;
                    this.typeUUIDKeyCache.put(event.typeUUID, keys);
                    if (keys.isEmpty()) {
                        throw new AdapterException("Failure in retrieving the Key field name for Table :" + dataType.getName() + ". At least one field should be assigned as primary key");
                    }
                    final Class<?> typeClass = (Class<?>)WALoader.get().loadClass(dataType.className);
                    fieldsOfThisTable = typeClass.getDeclaredFields();
                    this.typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
                }
                catch (Exception e) {
                    BaseDataStoreWriter.logger.warn((Object)("Unable to fetch the type for table " + event.metadata.get("TableName") + e));
                    throw new AdapterException("Unable to fetch the type for table " + event.metadata.get("TableName"), (Throwable)e);
                }
            }
        }
        final Map<String, Object> columnsAndDataMap = new LinkedHashMap<String, Object>();
        for (Integer i = 0; i < dataOrBeforeArray.length; ++i) {
            final boolean isPresent = BuiltInFunc.IS_PRESENT(event, dataOrBeforeArray, (int)i);
            if (isPresent) {
                if (fieldsOfThisTable != null) {
                    columnsAndDataMap.put(fieldsOfThisTable[i].getName(), (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
                }
                else {
                    columnsAndDataMap.put(i + "", (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
                }
            }
        }
        return new EventDataObject(columnsAndDataMap, event.metadata.get("OperationName").toString(), keys, this.currentReceivedPosition, fieldsOfThisTable);
    }
    
    protected synchronized void flushAll() throws Exception {
        for (final String table : this.targetDataMap.keySet()) {
            if (this.targetDataMap.get(table).size() > 0) {
                final List<Position> positions = this.writerProps.getWriterStrategy().write(table, new ArrayList<EventDataObject>(this.targetDataMap.get(table)));
                this.targetDataMap.get(table).clear();
                for (final Position pos : positions) {
                    this.writerProps.getCheckpointerStrategy().updatePosition(pos);
                }
                if (!BaseDataStoreWriter.logger.isDebugEnabled()) {
                    continue;
                }
                BaseDataStoreWriter.logger.debug((Object)("Flushed : " + table + "."));
            }
        }
        if (this.isRecoveryEnabled) {
            final Position pos2 = this.writerProps.getCheckpointerStrategy().getAckPosition();
            this.receiptCallback.ack((int)(Object)new Long(this.eventCounter), pos2);
            if (BaseDataStoreWriter.logger.isDebugEnabled()) {
                BaseDataStoreWriter.logger.debug((Object)("Checkpointed position : " + pos2 + "."));
            }
        }
        this.eventCounter = 0L;
    }
    
    public void close() throws Exception {
        super.close();
        if (this.writerProps.getBatchInterval() > 0L) {
            this.stopBatchTimer();
        }
        if (BaseDataStoreWriter.logger.isDebugEnabled()) {
            BaseDataStoreWriter.logger.debug((Object)"Flushing all data before close...");
        }
        try {
            this.flushAll();
        }
        catch (Exception e) {
            BaseDataStoreWriter.logger.warn((Object)("error while flushing the adapter " + e));
        }
        this.targetDataMap.clear();
        this.writerProps.getSourceTargetMap().clear();
        try {
            this.closeWriter();
        }
        catch (Exception e) {
            BaseDataStoreWriter.logger.warn((Object)("error while closing the adapter " + e));
        }
    }
    
    public abstract WriterProperties initWriter(final Map<String, Object> p0) throws Exception;
    
    public abstract void closeWriter() throws Exception;
    
    protected void initializeBatchTimer() {
        (this.batchTimer = new Timer("DataStoreBatchTimer")).schedule(new BatchTask(), this.writerProps.getBatchInterval(), this.writerProps.getBatchInterval());
    }
    
    protected void stopBatchTimer() {
        if (this.batchTimer != null) {
            this.batchTimer.cancel();
            this.batchTimer = null;
        }
    }
    
    protected void resetBatchIntervalTimer() {
        this.stopBatchTimer();
        this.initializeBatchTimer();
        if (BaseDataStoreWriter.logger.isDebugEnabled()) {
            BaseDataStoreWriter.logger.debug((Object)"Stopping and initializing timer");
        }
    }
    
    public long getEventCounter() {
        return this.eventCounter;
    }
    
    public long getBatchInterval() {
        return this.writerProps.getBatchInterval();
    }
    
    public long getBatchCount() {
        return this.writerProps.getBatchCount();
    }
    
    public Position getCurrentReceivedPosition() {
        return this.currentReceivedPosition;
    }
    
    public Map<String, String> getSourceTargetMap() {
        return this.writerProps.getSourceTargetMap();
    }
    
    public WriterProperties.PKUpdateHandlingMode getPKHandlingMode() {
        return this.writerProps.getPKUpdateHandlingMode();
    }
    
    public Map<String, ArrayList<EventDataObject>> getTargetDataMap() {
        return this.targetDataMap;
    }
    
    protected void handlePKUpdateEvents(final HDEvent event, final String sourceTableName) throws AdapterException, MetaDataRepositoryException, Exception {
        if (this.writerProps.getPKUpdateHandlingMode().equals(WriterProperties.PKUpdateHandlingMode.IGNORE)) {
            BaseDataStoreWriter.logger.warn((Object)("PK_UPDATE found on " + sourceTableName + ", ignoring event " + event));
            return;
        }
        if (this.writerProps.getPKUpdateHandlingMode().equals(WriterProperties.PKUpdateHandlingMode.ERROR)) {
            throw new AdapterException("PK_UPDATE found on " + sourceTableName + ", target set to handle PKUpdate with error, event affected " + event);
        }
        final List<EventDataObject> pkedos = new ArrayList<EventDataObject>();
        if (this.writerProps.getPKUpdateHandlingMode().equals(WriterProperties.PKUpdateHandlingMode.DELETEANDINSERT)) {
            event.metadata.put("OperationName", "DELETE");
            final Object[] originalData = event.data;
            event.data = event.before;
            final EventDataObject edo1 = this.getEventDataObject(event, event.data);
            event.metadata.put("OperationName", "INSERT");
            event.data = originalData;
            final EventDataObject edo2 = this.getEventDataObject(event, event.data);
            pkedos.add(edo1);
            pkedos.add(edo2);
        }
        for (final EventDataObject edo3 : pkedos) {
            if (this.getSourceTargetMap().containsKey(sourceTableName)) {
                this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo3);
            }
            else if (this.getSourceTargetMap().containsKey("*")) {
                this.targetDataMap.get(this.getSourceTargetMap().get("*")).add(edo3);
            }
            else {
                BaseDataStoreWriter.logger.warn((Object)("Ignoring event " + event + " No Mapping found, source not mapped with appropriate target"));
            }
        }
        pkedos.clear();
        ++this.eventCounter;
    }
    
    public String getTargetIdentifier(final Map<String, Object> prop) throws AdapterException {
        String targetId = null;
        final UUID targetUUID = (UUID)prop.get("TargetUUID");
        if (targetUUID == null) {
            BaseDataStoreWriter.logger.error((Object)"NULL TargetUUID");
            return null;
        }
        final MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
        try {
            final MetaInfo.MetaObject object = metadataRepository.getMetaObjectByUUID(targetUUID, HDSecurityManager.TOKEN);
            targetId = object.getFullName();
        }
        catch (MetaDataRepositoryException e) {
            throw new AdapterException("Failed to get complete details of target " + e.getMessage(), (Throwable)e);
        }
        return targetId;
    }
    
    static {
        BaseDataStoreWriter.logger = Logger.getLogger((Class)BaseDataStoreWriter.class);
    }
    
    private class BatchTask extends TimerTask
    {
        private Logger logger;
        
        private BatchTask() {
            this.logger = Logger.getLogger((Class)BatchTask.class);
        }
        
        @Override
        public void run() {
            synchronized (BaseDataStoreWriter.this.lockObject) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)"timer completed flushing data");
                }
                try {
                    BaseDataStoreWriter.this.flushAll();
                }
                catch (Exception ex) {
                    BaseDataStoreWriter.this.receiptCallback.notifyException(ex, (Event)null);
                }
            }
        }
    }
}
