package com.datasphere.proc;

import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.zeromq.ZMQException;

import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.ConnectionException;
import com.datasphere.common.exc.InvalidDataException;
import com.datasphere.common.exc.MetadataUnavailableException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.common.exc.TargetShutdownException;
import com.datasphere.event.Event;
import com.datasphere.exception.RuntimeInterruptedException;
import com.datasphere.exception.SourceShutdownException;
import com.datasphere.intf.EventSink;
import com.datasphere.intf.Process;
import com.datasphere.intf.Worker;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.records.Record;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.ReceiptCallback;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.security.Password;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

import zmq.ZError;

public abstract class BaseProcess implements Process
{
    private static Logger logger;
    protected transient List<EventSink> sinks;
    protected boolean running;
    String uri;
    protected String curAppName;
    protected ReceiptCallback receiptCallback;
    protected boolean isRecoveryEnabled;
    protected boolean typedEvent;
    protected final String TABLE_NAME = "TableName";
    protected final String OPERATION_NAME = "OperationName";
    protected final String OPERATION_VALUE = "INSERT";
    protected final String PK_UPDATE = "PK_UPDATE";
    protected MetaInfo.Type typeInfo;
    protected UUID typeUUID;
    protected final ClassLoader thisClassLoader;
    
    public BaseProcess() {
        this.sinks = new ArrayList<EventSink>();
        this.running = false;
        this.curAppName = null;
        this.isRecoveryEnabled = false;
        this.typedEvent = false;
        this.typeInfo = null;
        this.typeUUID = null;
        this.thisClassLoader = this.getClass().getClassLoader();
    }
    
    private void replacePropVariables(final Map<String, Object> properties, final String key, String value) {
        String namespace = null;
        if (value != null && !value.trim().isEmpty()) {
            namespace = this.getNameSpaceForRetrievingPropertyVariable(value, properties);
            value = this.getPropEnvKeyWithoutNameSpace(value);
        }
        MetaInfo.PropertyVariable propSet_from_mdr = null;
        try {
            propSet_from_mdr = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, value, null, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            BaseProcess.logger.error((Object)("The prop variable variable you are looking for is not set...Please set and use" + e.getMessage()));
        }
        final Map<String, Object> prop_from_mdr = propSet_from_mdr.getProperties();
        final Object obj = prop_from_mdr.get(value);
        final String plain_passwd = Password.getPlainStatic((String)obj);
        if (properties.get(key) instanceof Password) {
            properties.put(key, new Password(plain_passwd));
        }
        else {
            properties.put(key, plain_passwd);
        }
    }
    
    private boolean checkIfItIsPropertyVariable(String name, final Map<String, Object> properties) throws MetadataUnavailableException {
        String namespace = null;
        if (name != null && !name.trim().isEmpty()) {
            namespace = this.getNameSpaceForRetrievingPropertyVariable(name, properties);
            name = this.getPropEnvKeyWithoutNameSpace(name);
        }
        MetaInfo.PropertyVariable propSet_from_mdr = null;
        try {
            propSet_from_mdr = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, name, null, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            BaseProcess.logger.error((Object)("The prop variable variable you are looking for is not set...Please set and use" + e.getMessage()));
        }
        return propSet_from_mdr != null;
    }
    
    private String getNameSpaceForRetrievingPropertyVariable(final String propEnvKey, final Map<String, Object> properties) {
        String namespace = null;
        if (propEnvKey.contains(".")) {
            final String[] splitKeys = propEnvKey.split("\\.");
            namespace = splitKeys[0];
        }
        else {
            final UUID uuid = (UUID)properties.get("UUID");
            if (uuid == null) {
                BaseProcess.logger.warn((Object)"UUID not available in base process");
            }
            else {
                try {
                    final MetaInfo.MetaObject object = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    namespace = object.getNsName();
                }
                catch (MetaDataRepositoryException e) {
                    BaseProcess.logger.error((Object)("Metaobject not found with the uuid : " + uuid));
                }
            }
        }
        return namespace;
    }
    
    private String getPropEnvKeyWithoutNameSpace(final String propEnvKey) {
        if (propEnvKey.contains(".")) {
            final String[] splitKeys = propEnvKey.split("\\.");
            final String propName = splitKeys[1];
            return propName;
        }
        return propEnvKey;
    }
    
    private void replaceEnvVariableProperty(final Map<String, Object> properties) throws Exception {
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() instanceof String && entry.getValue().toString().trim().startsWith("$")) {
                final String propEnvKey = entry.getValue().toString().trim().substring(1);
                this.replaceEnvVariables(properties, entry.getKey(), propEnvKey);
            }
        }
    }
    
    private void replaceEnvVariables(final Map<String, Object> properties, final String key, String value) throws MetaDataRepositoryException {
        if (value != null && !value.trim().isEmpty()) {
            String defaultvalue = null;
            if (value.contains(":")) {
                final String[] splitKeys = value.split(":");
                value = splitKeys[0];
                defaultvalue = splitKeys[1];
                if (defaultvalue.startsWith("$")) {
                    defaultvalue = null;
                }
            }
            String propEnvValue = System.getProperty(value);
            if (propEnvValue == null) {
                propEnvValue = System.getenv(value);
            }
            if (propEnvValue != null) {
                if (properties.get(key) instanceof Password) {
                    properties.put(key, new Password(propEnvValue));
                }
                else {
                    properties.put(key, propEnvValue);
                }
            }
            else {
                if (defaultvalue == null) {
                    throw new MetaDataRepositoryException("No object found with the name :" + value + ". It could be a property variable or environment variable. Please create before using.");
                }
                properties.put(key, defaultvalue);
            }
        }
    }
    
    private void replacePropertyVariableOrEnvVariable(final Map<String, Object> properties) throws Exception {
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() instanceof String && entry.getValue().toString().trim().startsWith("$")) {
                final String propEnvKey = entry.getKey();
                final String propEnvValue = entry.getValue().toString().trim().substring(1);
                final boolean isPropertyVariable = this.checkIfItIsPropertyVariable(propEnvValue, properties);
                if (isPropertyVariable) {
                    this.replacePropVariables(properties, propEnvKey, propEnvValue);
                }
                else {
                    this.replaceEnvVariables(properties, propEnvKey, propEnvValue);
                }
            }
            if (entry.getValue() instanceof Password && ((Password)entry.getValue()).getPlain().startsWith("$")) {
                final Password p = (Password)entry.getValue();
                final String propEnvKey2 = entry.getKey();
                final String propEnvValue2 = p.getPlain().trim().substring(1);
                final boolean isPropertyVariable2 = this.checkIfItIsPropertyVariable(propEnvValue2, properties);
                if (isPropertyVariable2) {
                    this.replacePropVariables(properties, propEnvKey2, propEnvValue2);
                }
                else {
                    this.replaceEnvVariables(properties, propEnvKey2, propEnvValue2);
                }
            }
        }
    }
    
    @Override
    public void startWorker() {
        this.running = true;
        if (BaseProcess.logger.isInfoEnabled()) {
            BaseProcess.logger.info((Object)("Started process " + this.getClass().getCanonicalName()));
        }
    }
    
    @Override
    public void stopWorker() {
        this.running = false;
        if (BaseProcess.logger.isInfoEnabled()) {
            BaseProcess.logger.info((Object)("Stopped process " + this.getClass().getCanonicalName()));
        }
    }
    
    @Override
    public Worker.WorkerType getType() {
        return Worker.WorkerType.PROCESS;
    }
    
    @Override
    public String getUri() {
        return this.uri;
    }
    
    @Override
    public void setUri(final String uri) {
        BaseProcess.logger.warn((Object)(">> setting uri in base process : " + uri));
        this.uri = uri;
    }
    
    @Override
    public void close() throws Exception {
        if (BaseProcess.logger.isInfoEnabled()) {
            BaseProcess.logger.info((Object)("Closed process " + this.getClass().getCanonicalName()));
        }
    }
    
    @Override
    public void init(final Map<String, Object> properties) throws Exception {
        this.replaceEnvVariableProperty(properties);
    }
    
    @Override
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2) throws Exception {
        this.replacePropertyVariableOrEnvVariable(properties);
        this.replacePropertyVariableOrEnvVariable(properties2);
        this.init(properties);
    }
    
    @Override
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID) throws Exception {
        properties.put("UUID", sourceUUID);
        this.replacePropertyVariableOrEnvVariable(properties);
        this.replacePropertyVariableOrEnvVariable(properties2);
        this.init(properties, properties2);
    }
    
    @Override
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow) throws Exception {
        properties.put("UUID", sourceUUID);
        this.replacePropertyVariableOrEnvVariable(properties);
        this.replacePropertyVariableOrEnvVariable(properties2);
        this.init(properties, properties2, sourceUUID, distributionID);
    }
    
    @Override
    public void receive(final int channel, final Event event) throws Exception {
        final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.thisClassLoader);
            this.receiveImpl(channel, event);
        }
        catch (RecordException recoExp) {
            throw recoExp;
        }
        catch (InvalidDataException ide) {
            throw ide;
        }
        catch (AdapterException ae) {
            throw ae;
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (ZError.IOException | ClosedSelectorException | ZMQException ex3) {
            throw new InterruptedException();
        }
        catch (RuntimeInterruptedException e4) {
            throw new InterruptedException();
        }
        catch (SourceShutdownException sse) {
            throw sse;
        }
        catch (TargetShutdownException tse) {
            throw tse;
        }
        catch (MetadataUnavailableException | ConnectionException ex4) {
            throw ex4;
        }
        catch (Throwable t) {
            BaseProcess.logger.error((Object)(this.getClass().getCanonicalName() + "[" + this.getUri() + "] Problem processing event on channel " + channel + ": " + event), t);
            throw t;
        }
        finally {
            Thread.currentThread().setContextClassLoader(origialCl);
        }
    }
    
    @Override
    public void receive(final int channel, final Event event, final Position pos) throws Exception {
        this.receive(channel, event);
    }
    
    @Override
    public void receive(final ITaskEvent batch) throws Exception {
        this.receive(0, null);
    }
    
    public abstract void receiveImpl(final int p0, final Event p1) throws Exception;
    
    @Override
    public void addEventSink(final EventSink sink) {
        this.sinks.add(sink);
    }
    
    @Override
    public void removeEventSink(final EventSink sink) {
        this.sinks.remove(sink);
    }
    
    @Override
    public void send(final Event event, final int channel) throws Exception {
        this.send(event, channel, null);
    }
    
    @Override
    public void send(final Event event, final int channel, final Position pos) throws Exception {
        int sinkNo = 1;
        for (final EventSink sink : this.sinks) {
            if (channel == 0 || sinkNo++ == channel) {
                sink.receive(0, event, pos);
            }
        }
    }
    
    @Override
    public void send(final ITaskEvent batch) throws Exception {
        for (final EventSink sink : this.sinks) {
            sink.receive(batch);
        }
    }
    
    @Override
    public UUID getNodeID() {
        return HazelcastSingleton.getNodeId();
    }
    
    public void onDeploy(final Map<String, Object> properties1, final Map<String, Object> properties2, final Map<String, Object> parallelismProperties, final UUID uuid) throws Exception {
    }
    
    public void onDrop(final MetaInfo.MetaObject metaObject) throws Exception {
    }
    
    public void setReceiptCallback(final ReceiptCallback receiptCallback) {
        this.receiptCallback = receiptCallback;
    }
    
    public void setRecoveryEnabled(final boolean isRecoveryEnabled) {
        this.isRecoveryEnabled = isRecoveryEnabled;
    }
    
    public void flush() throws Exception {
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)"Default flush operation takes no action");
        }
    }
    
    protected MetaInfo.Type getTypeInfoFromStream(final UUID streamUUID) throws Exception {
        final MDRepository mdr = MetadataRepository.getINSTANCE();
        final MetaInfo.Stream streamInfo = (MetaInfo.Stream)mdr.getMetaObjectByUUID(streamUUID, HSecurityManager.TOKEN);
        this.typeUUID = streamInfo.dataType;
        this.curAppName = streamInfo.getCurrentApp().getFullName();
        this.typeInfo = (MetaInfo.Type)mdr.getMetaObjectByUUID(this.typeUUID, HSecurityManager.TOKEN);
        if (!this.typeInfo.className.equals("com.datasphere.proc.events.Record")) {
            this.typedEvent = true;
        }
        if (BaseProcess.logger.isDebugEnabled()) {
            BaseProcess.logger.debug((Object)(this.curAppName + ":type info = " + this.typeInfo.description));
        }
        return this.typeInfo;
    }
    
    protected Record convertTypedeventToRecord(final Event event) {
        Record Record = null;
        final Object[] payload = event.getPayload();
        if (payload != null) {
            final int payloadLength = payload.length;
            Record = new Record(payloadLength, (UUID)null);
            final HashMap<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("TableName", this.typeInfo.name);
            metadata.put("OperationName", "INSERT");
            Record.metadata = metadata;
            Record.typeUUID = this.typeUUID;
            Record.data = new Object[payloadLength];
            int i = 0;
            for (final Object o : payload) {
                Record.setData(i++, o);
            }
        }
        return Record;
    }
    
    public Position getWaitPosition() throws Exception {
        return null;
    }
    
    public String getDistributionId(final Event event) throws Exception {
        return null;
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
    }
    
    public void onCompile(final Compiler c, final MetaInfo.MetaObject metaObject) throws Exception {
    }
    
    public MetaInfo.MetaObject onUpgrade(final MetaInfo.MetaObject metaObject, final String fromVersion, final String toVersion) throws Exception {
        if (metaObject instanceof MetaInfo.Target) {
            final MetaInfo.Target targetMetaObject = (MetaInfo.Target)metaObject;
            this.updateAdapterProperties(targetMetaObject.properties, targetMetaObject.adapterClassName);
            return targetMetaObject;
        }
        if (metaObject instanceof MetaInfo.Source) {
            final MetaInfo.Source sourceMetaObject = (MetaInfo.Source)metaObject;
            this.updateAdapterProperties(sourceMetaObject.properties, sourceMetaObject.adapterClassName);
            return sourceMetaObject;
        }
        if (metaObject instanceof MetaInfo.Cache) {
            final MetaInfo.Cache cacheMetaObject = (MetaInfo.Cache)metaObject;
            this.updateAdapterProperties(cacheMetaObject.reader_properties, cacheMetaObject.adapterClassName);
            return cacheMetaObject;
        }
        throw new RuntimeException("Currently onUpgrade is supported for only Source, Cache and Target adapters.");
    }
    
    private void updateAdapterProperties(final Map<String, Object> adapterProperties, final String adapterClassName) throws Exception {
        final Class<?> cls = Class.forName(adapterClassName, false, ClassLoader.getSystemClassLoader());
        final PropertyTemplate anno = cls.getAnnotation(PropertyTemplate.class);
        final PropertyTemplateProperty[] properties;
        final PropertyTemplateProperty[] adapterPropertyTemplateProperties = properties = anno.properties();
        for (final PropertyTemplateProperty property : properties) {
            if (property.type() == Password.class) {
                final Object passwordProp = adapterProperties.get(property.name());
                if (passwordProp instanceof Map && ((Map)passwordProp).get(Constant.ENCRYPTED) != null) {
                    final Object encryptedPassword = ((Map)passwordProp).get(Constant.ENCRYPTED);
                    if (encryptedPassword instanceof String) {
                        final Password password = new Password();
                        password.setEncrypted((String)encryptedPassword);
                        adapterProperties.put(property.name(), password);
                        if (BaseProcess.logger.isInfoEnabled()) {
                            BaseProcess.logger.info((Object)("Property " + property.name() + " 's value of adapter " + anno.name() + " has been updated as required"));
                        }
                    }
                }
            }
        }
    }
    
    static {
        BaseProcess.logger = Logger.getLogger((Class)BaseProcess.class);
    }
}
