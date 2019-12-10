package com.datasphere.runtime.compiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.NotSet;
import com.datasphere.appmanager.ApplicationStatusResponse;
import com.datasphere.appmanager.ChangeApplicationStateResponse;
import com.datasphere.appmanager.FlowUtil;
import com.datasphere.classloading.HDLoader;
import com.datasphere.drop.DropMetaObject;
import com.datasphere.event.SimpleEvent;
import com.datasphere.exception.AlterException;
import com.datasphere.exception.CompilationException;
import com.datasphere.exception.FatalException;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.intf.AuthLayer;
import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.hazelcast.core.IMap;
import com.datasphere.errorhandling.DatallRuntimeException;
import com.datasphere.kafka.KafkaException;
import com.datasphere.metaRepository.FileMetadataExtensionRepository;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.ServerUpgradeUtility;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.HStorePersistencePolicy;
import com.datasphere.runtime.compiler.custom.AggHandlerDesc;
import com.datasphere.runtime.compiler.select.RSFieldDesc;
import com.datasphere.runtime.compiler.select.SelectCompiler;
import com.datasphere.runtime.compiler.stmts.ActionStmt;
import com.datasphere.runtime.compiler.stmts.AdapterDescription;
import com.datasphere.runtime.compiler.stmts.AlterDeploymentGroupStmt;
import com.datasphere.runtime.compiler.stmts.AlterStmt;
import com.datasphere.runtime.compiler.stmts.ConnectStmt;
import com.datasphere.runtime.compiler.stmts.CreateAdHocSelectStmt;
import com.datasphere.runtime.compiler.stmts.CreateAppOrFlowStatement;
import com.datasphere.runtime.compiler.stmts.CreateCacheStmt;
import com.datasphere.runtime.compiler.stmts.CreateCqStmt;
import com.datasphere.runtime.compiler.stmts.CreateDashboardStatement;
import com.datasphere.runtime.compiler.stmts.CreateDeploymentGroupStmt;
import com.datasphere.runtime.compiler.stmts.CreateNamespaceStatement;
import com.datasphere.runtime.compiler.stmts.CreatePropertySetStmt;
import com.datasphere.runtime.compiler.stmts.CreatePropertyVariableStmt;
import com.datasphere.runtime.compiler.stmts.CreateRoleStmt;
import com.datasphere.runtime.compiler.stmts.CreateShowSourceOrTargetStmt;
import com.datasphere.runtime.compiler.stmts.CreateShowStreamStmt;
import com.datasphere.runtime.compiler.stmts.CreateSorterStmt;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.datasphere.runtime.compiler.stmts.CreateStreamStmt;
import com.datasphere.runtime.compiler.stmts.CreateTypeStmt;
import com.datasphere.runtime.compiler.stmts.CreateUserStmt;
import com.datasphere.runtime.compiler.stmts.CreateVisualizationStmt;
import com.datasphere.runtime.compiler.stmts.CreateWASStmt;
import com.datasphere.runtime.compiler.stmts.CreateWindowStmt;
import com.datasphere.runtime.compiler.stmts.DeployStmt;
import com.datasphere.runtime.compiler.stmts.DeploymentRule;
import com.datasphere.runtime.compiler.stmts.DropStmt;
import com.datasphere.runtime.compiler.stmts.EndBlockStmt;
import com.datasphere.runtime.compiler.stmts.EventType;
import com.datasphere.runtime.compiler.stmts.ExecPreparedStmt;
import com.datasphere.runtime.compiler.stmts.ExportAppStmt;
import com.datasphere.runtime.compiler.stmts.ExportDataStmt;
import com.datasphere.runtime.compiler.stmts.ExportStreamSchemaStmt;
import com.datasphere.runtime.compiler.stmts.GrantPermissionToStmt;
import com.datasphere.runtime.compiler.stmts.GrantRoleToStmt;
import com.datasphere.runtime.compiler.stmts.ImportDataStmt;
import com.datasphere.runtime.compiler.stmts.ImportStmt;
import com.datasphere.runtime.compiler.stmts.LoadFileStmt;
import com.datasphere.runtime.compiler.stmts.LoadUnloadJarStmt;
import com.datasphere.runtime.compiler.stmts.MonitorStmt;
import com.datasphere.runtime.compiler.stmts.RevokePermissionFromStmt;
import com.datasphere.runtime.compiler.stmts.RevokeRoleFromStmt;
import com.datasphere.runtime.compiler.stmts.SecurityStmt;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.compiler.stmts.SetStmt;
import com.datasphere.runtime.compiler.stmts.SorterInOutRule;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.compiler.stmts.UpdateUserInfoStmt;
import com.datasphere.runtime.compiler.stmts.UseStmt;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.MetaObjectPermissionChecker;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.MetaInfoStatus;
import com.datasphere.runtime.meta.cdc.filters.FileTrailFilter;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.NameHelper;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.runtime.utils.StringUtils;
import com.datasphere.security.LDAPAuthLayer;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.Password;
import com.datasphere.security.HSecurityManager;
import com.datasphere.sourcefiltering.SourceSideFilteringHandler;
import com.datasphere.sourcefiltering.SourceSideFilteringManager;
import com.datasphere.tungsten.CluiMonitorView;
import com.datasphere.tungsten.Tungsten;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Compiler
{
    private static Logger logger;
    private final Grammar parser;
    private final Context ctx;
    MDRepository metadata_repo;
    private final String startingTime = "-START";
    private final String endingTime = "-END";
    private final String statusChecker = "-STATUS";
    private final String delimiter = ".";
    public static final String NAMED_QUERY_PREFIX;
    private static String COLON;
    
    public Compiler(final Grammar p, final Context ctx) {
        this.metadata_repo = MetadataRepository.getINSTANCE();
        this.parser = p;
        this.ctx = ctx;
    }
    
    public void error(final String message, final Object info) {
        if (this.parser != null) {
            this.parser.parseError(message, info);
            return;
        }
        throw new CompilationException(message);
    }
    
    public String getExprText(final Object o) {
        return (this.parser == null) ? "" : this.parser.lex.getExprText(o);
    }
    
    public Object compileStmt(final Stmt s) throws MetaDataRepositoryException {
        return s.compile(this);
    }
    
    public static void main(final String[] args) throws Exception {
        compile(AST.emptyStmt(), null, new ExecutionCallback() {
            @Override
            public void execute(final Stmt stmt, final Compiler compiler) throws Exception {
                System.out.println(stmt.getClass());
            }
        });
    }
    
    public static void compile(final String tqlText, final Context context, final ExecutionCallback cb) throws Exception {
        final Lexer lexer = new Lexer(tqlText, context.getIsUIContext());
        final Grammar parser = new Grammar(lexer, lexer.getSymbolFactory());
        final List<Stmt> stmts = parser.parseStmt(false);
        final Compiler compiler = new Compiler(parser, context);
        for (final Stmt stmt : stmts) {
            cb.execute(stmt, compiler);
        }
    }
    
    public static void compile(final Stmt stmt, final Context context, final ExecutionCallback cb) throws Exception {
        cb.execute(stmt, new Compiler(null, context));
    }
    
    public static void compile(final String tqlText, final Context context) throws Exception {
        compile(tqlText, context, new ExecutionCallback() {
            @Override
            public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                if (Compiler.logger.isDebugEnabled()) {
                    Compiler.logger.debug((Object)("parsed:\n" + stmt));
                }
                compiler.compileStmt(stmt);
            }
        });
    }
    
    public Context getContext() {
        return this.ctx;
    }
    
    public Class<?> getClass(final TypeName typename) {
        try {
            String typeName = typename.name.toString();
            try {
                final MetaInfo.Type t = this.getContext().getType(typeName);
                if (t != null && t.className != null) {
                    typeName = t.className;
                }
            }
            catch (MetaDataRepositoryException ex) {}
            Class<?> c;
            if (typename.array_dimensions > 0) {
                c = this.ctx.getClassWithoutReplacingPrimitives(typeName);
            }
            else {
                c = this.ctx.getClass(typeName);
            }
            for (int i = 0; i < typename.array_dimensions; ++i) {
                c = CompilerUtils.toArrayType(c);
            }
            return c;
        }
        catch (ClassNotFoundException e) {
            this.error("no such type", typename.name);
            return null;
        }
    }
    
    public MetaInfo.Type createAnonType(final UUID typeid, final Class<?> klass) {
        final MetaInfo.Type type = new MetaInfo.Type();
        type.construct(RuntimeUtils.genRandomName("type"), this.ctx.getCurNamespace(), klass.getName(), null, null, false);
        type.getMetaInfoStatus().setAnonymous(true);
        return type;
    }
    
    public Object compileCreateStreamStmt(final CreateStreamStmt stmt) throws MetaDataRepositoryException {
        UUID typeid;
        if (stmt.typeName != null) {
            assert stmt.fields == null;
            final MetaInfo.MetaObject type = this.ctx.getTypeInCurSchema(stmt.typeName);
            if (type == null) {
                try {
                    final Class<?> clazz = ClassLoader.getSystemClassLoader().loadClass(stmt.typeName);
                    typeid = this.getTypeForCLass(clazz).uuid;
                }
                catch (ClassNotFoundException e) {
                    this.error("no such type declaration", stmt.typeName);
                    typeid = null;
                }
            }
            else {
                typeid = type.uuid;
            }
        }
        else {
            assert stmt.fields != null;
            assert stmt.typeName == null;
            typeid = this.createType(false, RuntimeUtils.genRandomName("type"), stmt.fields, true);
        }
        this.checkPartitionFields(stmt.partitioning_fields, typeid, stmt.name);
        final MetaInfo.Type typeObject = (MetaInfo.Type)this.ctx.getObject(typeid);
        final String persistPropertySetName = this.getPersistencePolicyName(stmt.spp);
        final MetaInfo.Stream streamObject = this.ctx.putStream(stmt.doReplace, this.makeObjectName(stmt.name), typeid, stmt.partitioning_fields, stmt.gracePeriod, persistPropertySetName, null, null);
        this.setupKafka(streamObject, true);
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from type " + typeObject.name + " => stream " + streamObject.name + " } "));
        }
        this.addDependency(typeObject, streamObject);
        final ObjectName objectNameForStream = this.ctx.makeObjectName(stmt.name);
        final String streamName = getNameForTQL(this.ctx, objectNameForStream);
        final ObjectName objectNameForType = this.ctx.makeObjectName(stmt.typeName);
        final String typeName = getNameForTQL(this.ctx, objectNameForType);
        final String[] partitioning_fields = (String[])((stmt.partitioning_fields != null) ? ((String[])stmt.partitioning_fields.toArray(new String[stmt.partitioning_fields.size()])) : null);
        final String streamTxt = Utility.createStreamStatementText(streamName, stmt.doReplace, typeName, partitioning_fields, stmt.spp.getFullyQualifiedNameOfPropertyset());
        this.addSourceTextToMetaObject(streamTxt, streamObject);
        this.addCurrentApplicationFlowDependencies(typeObject);
        this.addCurrentApplicationFlowDependencies(streamObject);
        return streamObject;
    }
    
    private String getPersistencePolicyName(final StreamPersistencePolicy spp) throws MetaDataRepositoryException {
        String persistPropertySetName = null;
        if (spp == null) {
            return persistPropertySetName;
        }
        if (spp.getFullyQualifiedNameOfPropertyset() != null) {
            String namespace = Utility.splitDomain(spp.getFullyQualifiedNameOfPropertyset());
            if (namespace == null) {
                namespace = this.ctx.getCurNamespaceName();
            }
            final String name = Utility.splitName(spp.getFullyQualifiedNameOfPropertyset());
            final MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)this.metadata_repo.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, HSecurityManager.TOKEN);
            if (kpset == null) {
                throw new CompilationException("Couldn't find property set : " + namespace.concat(".").concat(name));
            }
            try {
                KafkaStreamUtils.validatePropertySet(kpset);
            }
            catch (KafkaException e) {
                e.printStackTrace();
                throw new MetaDataRepositoryException(e.getMessage());
            }
            persistPropertySetName = kpset.getFullName();
        }
        return persistPropertySetName;
    }
    
    private void setupKafka(final MetaInfo.Stream streamObject, final boolean doDelete) throws MetaDataRepositoryException {
        if (streamObject != null && streamObject.pset != null) {
            try {
                boolean did_create;
                if (HazelcastSingleton.isClientMember()) {
                    final RemoteCall createTopic_executor = KafkaStreamUtils.getCreateTopicExecutor(streamObject);
                    did_create = DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Boolean>)createTopic_executor);
                }
                else {
                    did_create = KafkaStreamUtils.createTopic(streamObject);
                }
                if (did_create) {
                    if (Compiler.logger.isInfoEnabled()) {
                        Compiler.logger.info((Object)("Created kafka topic with name: " + streamObject.getFullName()));
                    }
                    final MetaInfo.PropertySet kafka_propset = KafkaStreamUtils.getPropertySet(streamObject);
                    if (kafka_propset == null) {
                        throw new Exception("PropertySet " + streamObject.pset + " does not exist. Please create the propertySet and try again.");
                    }
                    streamObject.setPropertySet(kafka_propset);
                    final String format = (String)kafka_propset.properties.get("dataformat");
                    if (format != null && format.equalsIgnoreCase("avro")) {
                        final String schema = this.getSchema(format, streamObject);
                        streamObject.setAvroSchema(schema);
                    }
                    this.ctx.updateMetaObject(streamObject);
                }
                else {
                    Compiler.logger.error((Object)("Request to create kafka topic for stream: " + streamObject.getFullName() + " was successful but failed to verify its existence from zookeeper"));
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                if (doDelete) {
                    this.ctx.removeObject(streamObject);
                }
                Compiler.logger.error((Object)("Failed to create kafka topics associated with stream: " + streamObject.getFullName()), (Throwable)e);
                throw new RuntimeException("Failed to create kafka topics associated with stream: " + streamObject.getFullName() + ", Reason: " + e.getMessage(), e);
            }
        }
    }
    
    private String getSchema(final String format, final MetaInfo.Stream streamInfo) throws Exception {
        if (!format.equalsIgnoreCase("avro")) {
            return null;
        }
        final Map<String, Object> avroFormatter_Properties = new HashMap<String, Object>();
        final String schemFileName = streamInfo.getFullName().concat("_schema.avsc");
        avroFormatter_Properties.put("schemaFileName", schemFileName);
        final UUID type_uuid = streamInfo.getDataType();
        final MetaInfo.Type type = (MetaInfo.Type)this.metadata_repo.getMetaObjectByUUID(type_uuid, HSecurityManager.TOKEN);
        avroFormatter_Properties.put("TypeName", type.getName());
        final Class<?> typeClass = ClassLoader.getSystemClassLoader().loadClass(type.className);
        avroFormatter_Properties.put("EventType", "ContainerEvent");
        Field[] fields = typeClass.getDeclaredFields();
        final Field[] typedEventFields = new Field[fields.length - 1];
        int i = 0;
        for (final Field field : fields) {
            if (Modifier.isPublic(field.getModifiers())) {
                if (!"mapper".equals(field.getName())) {
                    typedEventFields[i] = field;
                    ++i;
                }
            }
        }
        fields = typedEventFields;
        final String formatterClassName = "com.datasphere.proc.AvroFormatter";
        final Class<?> formatterClass = Class.forName(formatterClassName, false, ClassLoader.getSystemClassLoader());
        formatterClass.getConstructor(Map.class, Field[].class).newInstance(avroFormatter_Properties, fields);
        final File schemaFile = new File(schemFileName);
        if (schemaFile.exists() && !schemaFile.isDirectory()) {
            final Schema schema = new Schema.Parser().parse(schemaFile);
            final String schemaString = schema.toString();
            schemaFile.delete();
            return schemaString;
        }
        throw new Exception("Avro Schema file by name " + schemaFile + " not found!");
    }
    
    public Object exportStreamSchema(final ExportStreamSchemaStmt exportStreamSchemaStmt) throws MetaDataRepositoryException {
        final String[] fullName = this.ctx.splitNamespaceAndName(exportStreamSchemaStmt.streamName, EntityType.STREAM);
        final MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)this.metadata_repo.getMetaObjectByName(EntityType.STREAM, fullName[0], fullName[1], null, HSecurityManager.TOKEN);
        if (streamMetaObject == null) {
            throw new RuntimeException("Stream " + exportStreamSchemaStmt.streamName + " not found!");
        }
        if (streamMetaObject.avroSchema == null) {
            throw new RuntimeException("No schema found for Stream " + streamMetaObject.getFullName());
        }
        final StringBuilder schemaFileNameBuilder = new StringBuilder();
        if (exportStreamSchemaStmt.optPath != null) {
            schemaFileNameBuilder.append(exportStreamSchemaStmt.optPath);
        }
        else {
            schemaFileNameBuilder.append(".");
        }
        if (exportStreamSchemaStmt.optFileName != null) {
            schemaFileNameBuilder.append("/").append(exportStreamSchemaStmt.optFileName).append(".avsc");
        }
        else {
            schemaFileNameBuilder.append("/").append(streamMetaObject.getNsName()).append("_").append(streamMetaObject.getName()).append("_schema.avsc");
        }
        try {
            final FileWriter fileWriter = new FileWriter(schemaFileNameBuilder.toString());
            fileWriter.write(streamMetaObject.avroSchema);
            fileWriter.close();
            return "Wrote schema for " + streamMetaObject.getFullName() + " to " + schemaFileNameBuilder.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
    private UUID createType(final boolean doReplace, final String typeName, final List<TypeField> fields, final boolean setAnonymous) throws MetaDataRepositoryException {
        return this.createType(doReplace, typeName, null, fields, setAnonymous);
    }
    
    private UUID createType(final boolean doReplace, final String typeName, final String extendsType, final List<TypeField> fields, final boolean setAnonymous) throws MetaDataRepositoryException {
        final Set<String> names = Factory.makeNameSet();
        final Map<String, String> fieldMap = Factory.makeLinkedMap();
        final List<String> keyFields = new ArrayList<String>();
        for (final TypeField tf : fields) {
            final String fieldName = tf.fieldName;
            if (names.contains(fieldName)) {
                this.error("duplicated field name", fieldName);
            }
            else {
                names.add(fieldName);
                final Class<?> c = this.getClass(tf.fieldType);
                fieldMap.put(fieldName, c.getName());
                if (!tf.isPartOfKey) {
                    continue;
                }
                keyFields.add(fieldName);
            }
        }
        final MetaInfo.Type type = this.ctx.putType(doReplace, this.makeObjectName(typeName), extendsType, fieldMap, keyFields, setAnonymous);
        this.addCurrentApplicationFlowDependencies(type);
        return type.uuid;
    }
    
    public UUID compileCreateTypeStmt(final CreateTypeStmt stmt) throws MetaDataRepositoryException {
        if (stmt.typeDef.typeName != null && stmt.typeDef.typeDef == null) {
            try {
                final Class<?> clazz = ClassLoader.getSystemClassLoader().loadClass(stmt.typeDef.typeName);
                return this.getTypeForCLass(clazz).uuid;
            }
            catch (ClassNotFoundException e) {
                this.error("external class could not be found", stmt.typeDef.typeName);
                return null;
            }
        }
        final UUID uuid = this.createType(stmt.doReplace, stmt.name, stmt.typeDef.extendsTypeName, stmt.typeDef.typeDef, false);
        final MetaInfo.MetaObject type = this.ctx.getObject(uuid);
        this.addSourceTextToMetaObject(stmt, type);
        return uuid;
    }
    
    public List<Field> checkPartitionFields(final List<String> partitionFields, final UUID typeId, final String streamName) throws MetaDataRepositoryException {
        final List<Field> ret = new ArrayList<Field>();
        if (partitionFields != null) {
            final Class<?> streamType = this.getTypeClass(typeId);
            final Set<String> dups = Factory.makeNameSet();
            for (final String fieldName : partitionFields) {
                if (dups.contains(fieldName)) {
                    this.error("field name is duplicated", fieldName);
                }
                else {
                    dups.add(fieldName);
                    try {
                        final Field fld = NamePolicy.getField(streamType, fieldName);
                        ret.add(fld);
                    }
                    catch (NoSuchFieldException e2) {
                        this.error("stream <" + streamName + "> has no such field", fieldName);
                    }
                    catch (SecurityException e) {
                        Compiler.logger.error((Object)e);
                    }
                }
            }
        }
        return ret;
    }
    
    private void validateWindowAttrPolicy(final UUID typeid, final String attrname) {
        MetaInfo.Type typeObj = null;
        try {
            typeObj = (MetaInfo.Type)this.ctx.getObject(typeid);
        }
        catch (MetaDataRepositoryException e) {
            this.error("Unable to get type for stream", null);
            return;
        }
        final String attrType = typeObj.fields.get(attrname);
        if (attrType == null) {
            this.error("Unable to find attribute in stream type.", attrname);
        }
        else if (attrType.equalsIgnoreCase("java.lang.String") || attrType.equalsIgnoreCase("String")) {
            this.error("Attribute type is not comparable, hence cannot be used for attribute time", attrname);
        }
    }
    
    public Object compileCreateWindowStmt(final CreateWindowStmt stmt) throws MetaDataRepositoryException {
        final MetaInfo.Stream stream = this.ctx.getStreamInCurSchema(stmt.stream_name);
        if (stream == null) {
            this.error("no such stream", stmt.stream_name);
        }
        final Object stats = stmt.getOption("dumpstats");
        Integer dumpstatsVal;
        if (stats instanceof Number) {
            dumpstatsVal = ((Number)stats).intValue();
        }
        else if (stats instanceof String) {
            dumpstatsVal = Integer.valueOf((String)stats);
        }
        else if (stmt.haveOption("dumpstats")) {
            dumpstatsVal = 0;
        }
        else {
            dumpstatsVal = null;
        }
        if (stmt.window_len.first != null) {
            final IntervalPolicy policy = stmt.window_len.first;
            if (policy.isAttrBased()) {
                final IntervalPolicy.AttrBasedPolicy attrPolicy = policy.getAttrPolicy();
                this.validateWindowAttrPolicy(stream.getDataType(), attrPolicy.getAttrName());
            }
        }
        this.checkPartitionFields(stmt.partitioning_fields, stream.dataType, stmt.stream_name);
        final MetaInfo.Window windowObject = this.ctx.putWindow(stmt.doReplace, this.makeObjectName(stmt.name), stream.uuid, stmt.partitioning_fields, stmt.window_len, stmt.jumping, false, false, dumpstatsVal);
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from stream " + stream.name + " => window " + windowObject.name + " } "));
        }
        this.addDependency(stream, windowObject);
        this.addCurrentApplicationFlowDependencies(stream);
        this.addCurrentApplicationFlowDependencies(windowObject);
        this.addSourceTextToMetaObject(stmt, windowObject);
        return null;
    }
    
    public static TraceOptions buildTraceOptions(final Stmt stmt) {
        int traceFlags = 0;
        Object traceFilename = null;
        Object traceFilePath = null;
        if (stmt != null) {
            if (stmt.haveOption("dumpinput")) {
                traceFlags |= 0x1;
            }
            if (stmt.haveOption("dumpinputx")) {
                traceFlags |= 0x21;
            }
            if (stmt.haveOption("dumpoutput")) {
                traceFlags |= 0x2;
            }
            if (stmt.haveOption("dumpcode")) {
                traceFlags |= 0x4;
            }
            if (stmt.haveOption("dumpplan")) {
                traceFlags |= 0x8;
            }
            if (stmt.haveOption("dumpproc")) {
                traceFlags |= 0x10;
            }
            if (stmt.haveOption("DEBUGINFO")) {
                traceFlags |= 0x40;
            }
            traceFilename = stmt.getOption("dumptofile");
            traceFilePath = stmt.getOption("dumpcode");
        }
        final TraceOptions traceOptions = new TraceOptions(traceFlags, (String)traceFilename, (String)traceFilePath);
        return traceOptions;
    }
    
    private String fixFieldName(final String name) {
        if (name == null) {
            throw new RuntimeException("Field name is null");
        }
        final StringBuilder fixedName = new StringBuilder();
        for (int i = 0; i < name.length(); ++i) {
            if ((i == 0 && Character.isJavaIdentifierStart(name.charAt(i))) || (i != 0 && Character.isJavaIdentifierPart(name.charAt(i)))) {
                fixedName.append(name.charAt(i));
            }
        }
        return fixedName.toString();
    }
    
    private UUID genStreamType(final CQExecutionPlan plan, final CreateCqStmt stmt) {
        final List<TypeField> makeFieldList = new ArrayList<TypeField>();
        final List<RSFieldDesc> rfields = plan.resultSetDesc;
        if (rfields.size() == 1) {
            final Class<?> fldType = rfields.get(0).type;
            if (SimpleEvent.class.isAssignableFrom(fldType)) {
                try {
                    return this.getTypeForCLass(fldType).uuid;
                }
                catch (MetaDataRepositoryException ex) {}
            }
        }
        final Map<String, String> fields = new LinkedHashMap<String, String>();
        for (final RSFieldDesc resultFields : rfields) {
            String name = resultFields.name;
            String typeClassName = resultFields.type.getCanonicalName();
            if (typeClassName.equals("com.datasphere.runtime.compiler.CompilerUtils.NullType")) {
                typeClassName = "java.lang.Object";
            }
            name = this.fixFieldName(name);
            for (int count = 2; fields.get(name) != null; name += count++) {}
            fields.put(name, typeClassName);
            makeFieldList.add(new TypeField(name, new TypeName(typeClassName, 0), false));
        }
        try {
            final MetaInfoStatus status = new MetaInfoStatus().setAnonymous(false).setGenerated(true);
            final ObjectName objectName = this.ctx.makeObjectName(Utility.splitDomain(stmt.stream_name), Utility.splitName(stmt.stream_name));
            final MetaInfo.Type typeGenerated = this.ctx.putType(true, this.makeObjectName(objectName.getNamespace(), objectName.getName() + "_Type"), null, fields, null, status);
            final String typeName = getNameForTQL(this.ctx, objectName);
            this.addSourceTextToMetaObject(Utility.createTypeStatementText(typeName + "_Type", stmt.doReplace, makeFieldList), typeGenerated);
            return typeGenerated.getUuid();
        }
        catch (MetaDataRepositoryException e) {
            Compiler.logger.warn((Object)e.getLocalizedMessage());
            return null;
        }
    }
    
    private MetaInfo.Stream createStreamFromCQ(final CreateCqStmt stmt, final UUID oldUUIDforGeneratedStream) throws Exception {
        final Select copyOfSelect = stmt.select.copyDeep();
        final CQExecutionPlan plan = SelectCompiler.compileSelect(Utility.splitName(stmt.name), this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)), this, copyOfSelect, buildTraceOptions(stmt));
        final UUID streamType = this.genStreamType(plan, stmt);
        final MetaInfo.Type streamTypeMetaObject = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(streamType, this.ctx.getAuthToken());
        try {
            final MetaInfoStatus status = new MetaInfoStatus().setAnonymous(true).setGenerated(true);
            final String persistPropertySetName = this.getPersistencePolicyName(stmt.getPersistencePolicy());
            final MetaInfo.Stream str = this.ctx.putStream(true, this.makeObjectName(this.ctx.getNamespaceName(Utility.splitDomain(stmt.stream_name)), stmt.stream_name), streamType, stmt.getPartitionFieldList(), null, persistPropertySetName, oldUUIDforGeneratedStream, status);
            this.setupKafka(str, true);
            String[] partitionFieldList = null;
            if (stmt.getPartitionFieldList() != null) {
                partitionFieldList = stmt.getPartitionFieldList().toArray(new String[0]);
            }
            final ObjectName objectName = this.ctx.makeObjectName(Utility.splitDomain(stmt.stream_name), Utility.splitName(stmt.stream_name));
            final String streamName = getNameForTQL(this.ctx, objectName);
            this.addSourceTextToMetaObject(Utility.createStreamStatementText(streamName, stmt.doReplace, streamTypeMetaObject.getName(), partitionFieldList, persistPropertySetName), str);
            this.addDependency(streamTypeMetaObject, str);
            return str;
        }
        catch (MetaDataRepositoryException e) {
            Compiler.logger.warn((Object)e.getLocalizedMessage());
            return null;
        }
    }
    
    public Object compileCreateCqStmt(final CreateCqStmt stmt) throws MetaDataRepositoryException {
        MetaInfo.MetaObject dataSink = this.ctx.getDataSourceInCurSchema(stmt.stream_name);
        UUID oldUUIDforGeneratedStream = null;
        String cqString = stmt.sourceText;
        final String name = this.ctx.addSchemaPrefix(this.ctx.makeObjectName(stmt.name));
        final MetaInfo.CQ cq = this.ctx.getCQ(name);
        if (cq != null && !this.ctx.recompileMode && !stmt.doReplace) {
            throw new CompilationException("Continuous query " + name + " already exists");
        }
        if (dataSink != null && dataSink instanceof MetaInfo.Stream && dataSink.getMetaInfoStatus().isAnonymous() && dataSink.getMetaInfoStatus().isGenerated()) {
            if (this.ctx.recompileMode) {
                oldUUIDforGeneratedStream = dataSink.getUuid();
            }
            if (stmt.doReplace && !this.ctx.recompileMode && !ServerUpgradeUtility.isUpgrading) {
                dataSink = null;
            }
        }
        final ObjectName objectName = this.makeObjectName(stmt.stream_name);
        final String streamName = getNameForTQL(this.ctx, objectName);
        cqString = Utility.createCQStatementText(stmt.name, stmt.doReplace, streamName, null, stmt.select_text);
        if (dataSink == null) {
            try {
                if (!ServerUpgradeUtility.isUpgrading) {
                    dataSink = this.createStreamFromCQ(stmt, oldUUIDforGeneratedStream);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        UUID outputTypeId = null;
        if (dataSink instanceof MetaInfo.Stream) {
            outputTypeId = ((MetaInfo.Stream)dataSink).dataType;
        }
        else if (dataSink instanceof MetaInfo.HDStore) {
            outputTypeId = ((MetaInfo.HDStore)dataSink).contextType;
        }
        else {
            this.error("data sink should be stream or hdstore", stmt.stream_name);
        }
        final MetaInfo.Type streamType = this.getTypeInfo(outputTypeId);
        final Class<?> streamClass = this.getTypeClass(outputTypeId);
        final Set<String> fieldSet = Factory.makeNameSet();
        final List<Field> targetFields = new ArrayList<Field>();
        for (final String fieldName : stmt.field_list) {
            if (fieldSet.contains(fieldName)) {
                this.error("field name is duplicated", fieldName);
            }
            else {
                fieldSet.add(fieldName);
                try {
                    final Field f = NamePolicy.getField(streamClass, fieldName);
                    targetFields.add(f);
                }
                catch (NoSuchFieldException | SecurityException ex2) {
                    this.error("no such field in target stream", fieldName);
                }
            }
        }
        final Map<String, Integer> targetFieldIndices = new HashMap<String, Integer>();
        int index = 0;
        for (final String fieldName2 : streamType.fields.keySet()) {
            targetFieldIndices.put(fieldName2, index++);
        }
        MetaInfo.CQ cqObject = null;
        UUID outputStreamId = null;
        try {
            final CQExecutionPlan plan = SelectCompiler.compileSelectInto(Utility.splitName(stmt.name), this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)), this, stmt.select, buildTraceOptions(stmt), streamClass, targetFields, targetFieldIndices, outputTypeId, false);
            if (this.ctx.isReadOnly()) {
                return null;
            }
            outputStreamId = dataSink.uuid;
            cqObject = this.ctx.putCQ(stmt.doReplace, this.makeObjectName(stmt.name), outputStreamId, plan, stmt.select_text, stmt.field_list, null, stmt.uiConfig);
        }
        catch (Exception e3) {
            throw new RuntimeException(e3);
        }
        for (final CQExecutionPlan.DataSource ds : cqObject.plan.dataSources) {
            if (Compiler.logger.isDebugEnabled()) {
                Compiler.logger.debug((Object)("Added dependency: { from stream/window/cache " + ds.name + " => cq " + cqObject.name + " } "));
            }
            MetaInfo.MetaObject subTaskMetaObject = this.ctx.getDataSourceInCurSchema(ds.name);
            if (subTaskMetaObject == null) {
                final UUID dsid = ds.getDataSourceID();
                subTaskMetaObject = this.ctx.getObject(dsid);
            }
            this.addDependency(subTaskMetaObject, cqObject);
            this.addCurrentApplicationFlowDependencies(subTaskMetaObject);
        }
        final MetaInfo.MetaObject outputStreamMetaObject = this.ctx.getObject(outputStreamId);
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from stream/hdstore " + outputStreamMetaObject.name + " => cq " + cqObject.name + " } "));
        }
        this.addDependency(outputStreamMetaObject, cqObject);
        this.addCurrentApplicationFlowDependencies(outputStreamMetaObject);
        this.addCurrentApplicationFlowDependencies(cqObject);
        if (dataSink instanceof MetaInfo.Stream && dataSink.getMetaInfoStatus().isGenerated()) {
            this.addSourceTextToMetaObject(cqString, cqObject);
        }
        else {
            this.addSourceTextToMetaObject(stmt, cqObject);
        }
        if (ServerUpgradeUtility.isUpgrading && dataSink instanceof MetaInfo.Stream && dataSink.getMetaInfoStatus().isGenerated()) {
            this.revalidateTheComponents(cqObject, stmt.doReplace);
        }
        return cqObject;
    }
    
    public static String getNameForTQL(final Context ctx, final ObjectName objectName) {
        String streamName;
        if (ctx.getCurNamespace() != null && ctx.getCurNamespace().getName().equalsIgnoreCase(objectName.getNamespace())) {
            streamName = objectName.getName();
        }
        else {
            streamName = objectName.getFullName();
        }
        return streamName;
    }
    
    private void addCurrentApplicationFlowDependencies(final MetaInfo.MetaObject metaObject) throws MetaDataRepositoryException {
        final MetaInfo.Flow currentApp = this.ctx.getCurApp();
        final MetaInfo.Flow currentFlow = this.ctx.getCurFlow();
        if (currentApp != null) {
            if (Compiler.logger.isDebugEnabled()) {
                Compiler.logger.debug((Object)("Added dependency: { from " + metaObject.type.toString() + " " + metaObject.name + " => currentApp " + currentApp.name + " } " + currentApp.uuid));
            }
            this.addDependency(metaObject, currentApp);
        }
        if (currentFlow != null) {
            if (Compiler.logger.isDebugEnabled()) {
                Compiler.logger.debug((Object)("Added dependency: { from " + metaObject.type.toString() + " " + metaObject.name + " => currentFlow " + currentFlow.name + " } " + currentFlow.uuid));
            }
            this.addDependency(metaObject, currentFlow);
        }
    }
    
    private void addSourceTextToMetaObject(final String sourceText, final MetaInfo.MetaObject metaObject) throws MetaDataRepositoryException {
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("addSourceTextToMetaObject() " + metaObject + " " + sourceText));
        }
        metaObject.setSourceText(sourceText);
        this.metadata_repo.updateMetaObject(metaObject, this.ctx.getSessionID());
    }
    
    private void addSourceTextToMetaObject(final Stmt stmt, final MetaInfo.MetaObject metaObject) throws MetaDataRepositoryException {
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("addSourceTextToMetaObject() " + metaObject + " " + stmt.sourceText));
        }
        metaObject.setSourceText(stmt.sourceText);
        this.metadata_repo.updateMetaObject(metaObject, this.ctx.getSessionID());
    }
    
    public Object compileShowStreamStmt(final CreateShowStreamStmt stmt) throws MetaDataRepositoryException {
        final MetaInfo.Stream stream = this.ctx.getStreamInCurSchema(stmt.stream_name);
        if (stream == null) {
            this.error("Unable to find stream", stmt.stream_name);
        }
        final UUID session_id = (UUID)this.ctx.getSessionID();
        final MetaInfo.ShowStream show_stream = new MetaInfo.ShowStream();
        show_stream.construct(true, stmt.line_count, stream.uuid, stmt.isTungsten, session_id);
        this.ctx.showStreamStmt(show_stream);
        return null;
    }
    
    public Object compileShowSourceOrTargetStmt(final CreateShowSourceOrTargetStmt stmt) throws MetaDataRepositoryException {
        int totalRows = 0;
        String messageToConsole = null;
        String appName = null;
        UUID uuid = null;
        final Set<String> definedSetOfProps = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        definedSetOfProps.addAll(Arrays.asList("-START", "-END", "-STATUS"));
        final String componentName = stmt.getComponentName();
        final List<String> props = stmt.getProps();
        int limit = stmt.getLimit();
        final boolean isDescending = stmt.isDescending();
        String nameSpace = Utility.splitDomain(componentName);
        if (nameSpace == null) {
            nameSpace = this.ctx.getCurNamespaceName();
        }
        final MetaInfo.MetaObject obj1 = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.SOURCE, nameSpace, Utility.splitName(componentName), null, this.ctx.getAuthToken());
        final MetaInfo.MetaObject obj2 = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.TARGET, nameSpace, Utility.splitName(componentName), null, this.ctx.getAuthToken());
        final MetaInfo.MetaObject obj3 = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.CACHE, nameSpace, Utility.splitName(componentName), null, this.ctx.getAuthToken());
        if (obj1 == null && obj2 == null && obj3 == null) {
            throw new CompilationException("There is no source or target component with the name : " + componentName + " in namespace : " + nameSpace + ". Please check the name of the component and submit the command again.");
        }
        if (obj1 != null) {
            final MetaInfo.Flow flow = MetaInfo.MetaObject.getCurrentApp(obj1);
            appName = flow.getName();
            uuid = obj1.getUuid();
        }
        else if (obj2 != null) {
            final MetaInfo.Flow flow = MetaInfo.MetaObject.getCurrentApp(obj2);
            appName = flow.getName();
            uuid = obj2.getUuid();
        }
        else {
            final MetaInfo.Flow flow = MetaInfo.MetaObject.getCurrentApp(obj3);
            appName = flow.getName();
            uuid = obj3.getUuid();
        }
        if (limit == Integer.MAX_VALUE) {
            limit = 10;
            totalRows = FileMetadataExtensionRepository.getInstance().getTotalRowsInFileMetadataRepo(componentName, uuid);
            if (totalRows <= limit) {
                limit = totalRows;
            }
            messageToConsole = "Showing " + limit + " rows, out of a total of " + totalRows + " rows. Please use Limit keyword in the show command to control the number of rows displayed.";
        }
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Meta object found with the name : " + componentName + ". Fetching the trail of the component."));
        }
        if (props == null) {
            final Set<FileMetadataExtension> result = this.getTrailFromFileMetadataExtension(nameSpace + "." + Utility.splitName(componentName), uuid, null, limit, null, null, null, isDescending);
            return new FileMetadataExtensionResult(result, messageToConsole, appName);
        }
        final Map<String, Object> propertyMap = this.verifyArgNamesAndValues(props);
        if (propertyMap == null) {
            return null;
        }
        final boolean check = this.validatePropsForShowSourceOrTargetStmt(propertyMap, definedSetOfProps);
        if (check) {
            final Set<FileMetadataExtension> result2 = this.getTrailFromFileMetadataExtension(nameSpace + "." + Utility.splitName(componentName), uuid, propertyMap, limit, "-START", "-END", "-STATUS", isDescending);
            return new FileMetadataExtensionResult(result2, messageToConsole, appName);
        }
        return null;
    }
    
    private Set<FileMetadataExtension> getTrailFromFileMetadataExtension(final String componentName, final UUID uuid, final Map<String, Object> propertyMap, final int limit, final String startTime, final String endTime, final String status, final boolean isDescending) {
        String startTimeValue = null;
        String endTimeValue = null;
        String statusValue = null;
        if (propertyMap != null) {
            if (propertyMap.containsKey(startTime)) {
                startTimeValue = (String)propertyMap.get(startTime);
            }
            if (propertyMap.containsKey(endTime)) {
                endTimeValue = (String)propertyMap.get(endTime);
            }
            if (propertyMap.containsKey(status)) {
                statusValue = (String)propertyMap.get(status);
            }
        }
        final FileMetadataExtensionRepository fileMetadataExtensionRepository = FileMetadataExtensionRepository.getInstance();
        return fileMetadataExtensionRepository.queryFileMetadataExtension(this.ctx.getAuthToken(), new FileTrailFilter(componentName, startTimeValue, endTimeValue, statusValue, limit, isDescending, uuid));
    }
    
    private boolean validatePropsForShowSourceOrTargetStmt(final Map<String, Object> propertyMap, final Set<String> definedSetOfProps) throws MetaDataRepositoryException {
        for (final String keyName : propertyMap.keySet()) {
            if (!definedSetOfProps.contains(keyName)) {
                throw new CompilationException("The search parameter : " + keyName + " is not supported. Allowed search parameters are \"STARTTIME\", \"ENDTIME\", \"STATUS\"");
            }
        }
        return true;
    }
    
    public MetaInfo.Query compileCreateAdHocSelect(final CreateAdHocSelectStmt stmt) throws MetaDataRepositoryException {
        String alias = stmt.name;
        final boolean isAdhoc = alias == null;
        if (!isAdhoc) {
            final MetaInfo.MetaObject obj = this.ctx.get(alias, EntityType.QUERY);
            if (obj != null) {
                throw new CompilationException(obj.type + " with name <" + obj.getFullName() + "> already exists");
            }
        }
        final String copyOfAlias = alias;
        final String queryNS = this.ctx.makeObjectName(copyOfAlias).getNamespace();
        alias = this.ctx.makeObjectName(copyOfAlias).getName();
        String queryName;
        String appName;
        String cqName;
        String streamName;
        if (!isAdhoc) {
            queryName = alias;
            appName = Compiler.NAMED_QUERY_PREFIX + alias;
            cqName = Compiler.NAMED_QUERY_PREFIX + alias;
            streamName = Compiler.NAMED_QUERY_PREFIX + alias;
        }
        else {
            queryName = (appName = RuntimeUtils.genRandomName("adhocquery"));
            cqName = RuntimeUtils.genRandomName("cq");
            streamName = RuntimeUtils.genRandomName("stream");
        }
        try {
            final CQExecutionPlan.CQExecutionPlanFactory planFactory = new CQExecutionPlan.CQExecutionPlanFactory() {
                @Override
                public CQExecutionPlan createPlan() throws Exception {
                    final CQExecutionPlan plan = SelectCompiler.compileSelect(cqName, queryNS, Compiler.this, stmt.select, Compiler.buildTraceOptions(stmt), isAdhoc);
                    return plan;
                }
            };
            final MetaInfo.Query query = this.ctx.buildAdHocQuery(appName, streamName, cqName, queryName, planFactory, stmt.select_text, stmt.sourceText, isAdhoc, null, queryNS);
            return query;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public Object compileDropStmt(final DropStmt stmt) throws MetaDataRepositoryException {
        if (stmt.objectName.split("\\.").length > 2) {
            this.error(stmt.objectName + " format is wrong", stmt.objectName);
        }
        final List<String> str = this.ctx.dropObject(stmt.objectName, stmt.objectType, stmt.dropRule);
        stmt.returnText = str;
        return null;
    }
    
    private Class<?> loadClass(final String className) {
        try {
            final Class<?> c = this.ctx.loadClass(className);
            return c;
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }
    
    private static boolean canMethodBeImported(final Method m) {
        return javassist.Modifier.isStatic(m.getModifiers()) || (javassist.Modifier.isAbstract(m.getModifiers()) && m.getAnnotation(AggHandlerDesc.class) != null);
    }
    
    public Object compileImportStmt(final ImportStmt importStmt) {
        final Imports im = this.ctx.getImports();
        final String name = importStmt.name;
        if (importStmt.hasStaticKeyword) {
            if (importStmt.hasStar) {
                final String className = name;
                final Class<?> c = this.loadClass(className);
                if (c == null) {
                    this.error("cannot resolve type", name);
                }
                else {
                    final Field[] fields;
                    final Field[] declFields = fields = c.getFields();
                    for (final Field f : fields) {
                        if (javassist.Modifier.isStatic(f.getModifiers())) {
                            final Field pf = im.addStaticFieldRef(f);
                            if (pf != null) {
                                this.error("import of static field hides previous import", name);
                            }
                        }
                    }
                    final Method[] declaredMethods;
                    final Method[] declMethods = declaredMethods = c.getDeclaredMethods();
                    for (final Method m : declaredMethods) {
                        if (canMethodBeImported(m)) {
                            im.addStaticMethod(c, m.getName());
                        }
                    }
                }
            }
            else {
                final String className = NameHelper.getPrefix(name);
                final String varName = NameHelper.getBasename(name);
                if (className.isEmpty()) {
                    this.error("invalid import declaration", name);
                }
                else {
                    final Class<?> c2;
                    if ((c2 = this.loadClass(className)) == null) {
                        this.error("cannot resolve type " + className, name);
                    }
                    else {
                        boolean hasField = false;
                        boolean hasMethod = false;
                        try {
                            final Field f2 = NamePolicy.getField(c2, varName);
                            if (javassist.Modifier.isStatic(f2.getModifiers())) {
                                final Field pf2 = im.addStaticFieldRef(f2);
                                if (pf2 != null) {
                                    this.error("import of static field hides previous import", name);
                                }
                                hasField = true;
                            }
                        }
                        catch (NoSuchFieldException ex) {}
                        final Method[] declaredMethods2;
                        final Method[] declMethods2 = declaredMethods2 = c2.getDeclaredMethods();
                        for (final Method i : declaredMethods2) {
                            if (canMethodBeImported(i) && i.getName().equals(varName)) {
                                im.addStaticMethod(c2, i.getName());
                                hasMethod = true;
                            }
                        }
                        if (!hasField && !hasMethod) {
                            this.error("type has not member " + varName, name);
                        }
                    }
                }
            }
        }
        else if (importStmt.hasStar) {
            im.importPackage(name.toString());
        }
        else {
            final String className = name.toString();
            try {
                final Class<?> c = this.ctx.loadClass(className);
                final Class<?> pc = im.importClass(c);
                if (pc != null) {
                    this.error("import of class hides previous import", name);
                }
            }
            catch (ClassNotFoundException e) {
                this.error("cannot resolve type", name);
            }
        }
        return null;
    }
    
    public Class<?> getTypeClass(final UUID type) throws MetaDataRepositoryException {
        return this.ctx.getTypeClass(type);
    }
    
    public MetaInfo.Type getTypeInfo(final UUID type) throws MetaDataRepositoryException {
        return this.ctx.getTypeInfo(type);
    }
    
    public Object compileUseStmt(final UseStmt stmt) throws MetaDataRepositoryException {
        if (stmt.what == EntityType.NAMESPACE) {
            this.ctx.useNamespace(stmt.schemaName);
        }
        else {
            if (stmt.what == EntityType.QUERY) {
                try {
                    this.ctx.recompileQuery(stmt.schemaName);
                    return null;
                }
                catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            this.ctx.alterAppOrFlow(stmt.what, stmt.schemaName, stmt.recompile);
        }
        return null;
    }
    
    private Map<String, Object> loadProperties(final String name) throws MetaDataRepositoryException {
        final MetaInfo.PropertySet props = this.ctx.getPropertySetInCurSchema(name);
        if (props == null) {
            this.error("cannot find properties", name);
            return Collections.emptyMap();
        }
        return props.properties;
    }
    
    public Map<String, Object> verifyArgNamesAndValues(final List<String> list) {
        final Map<String, Object> props = Factory.makeCaseInsensitiveMap();
        if (list != null) {
            for (int i = 0; i < list.size(); i += 2) {
                final String keyName = list.get(i);
                if (i + 1 >= list.size()) {
                    System.out.println(keyName + " requires an argument");
                    return null;
                }
                if (keyName.equalsIgnoreCase("-STATUS")) {
                    if (!this.verifyEnumValuesForStatusField(list.get(i + 1))) {
                        return null;
                    }
                }
                else if ((keyName.equalsIgnoreCase("-START") || keyName.equals("-END")) && !this.verifyIfDateFormatIsCorrect(list.get(i + 1))) {
                    return null;
                }
                props.put(list.get(i), list.get(i + 1));
            }
        }
        return props;
    }
    
    private boolean verifyEnumValuesForStatusField(final String value) {
        for (final FileMetadataExtension.Status statusValue : FileMetadataExtension.Status.values()) {
            if (statusValue.name().equalsIgnoreCase(value)) {
                return true;
            }
        }
        System.out.println("Message : Status can only have the following values.\n1. CREATED,\n2. COMPLETED,\n3. CRASHED, \n4. PROCESSING \n");
        return false;
    }
    
    private boolean verifyIfDateFormatIsCorrect(final String date) {
        final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            df.parse(date);
            return true;
        }
        catch (ParseException e) {
            System.out.println("Message : The value for StartTime/EndTime should be of the format yyyy-mm-dd");
            return false;
        }
    }
    
    public Map<String, Object> combineProperties(final List<Property> propList) throws MetaDataRepositoryException {
        final Map<String, Object> props = Factory.makeCaseInsensitiveMap();
        if (propList != null) {
            for (final Property p : propList) {
                if (p.name.equals("#")) {
                    final Map<String, Object> pp = this.loadProperties((String)p.value);
                    props.putAll(pp);
                }
                else if (p.value instanceof List) {
                    final Object value = this.combineProperties((List<Property>)p.value);
                    props.put(p.name, value);
                }
                else {
                    props.put(p.name, p.value);
                }
            }
        }
        return this.encryptSensitiveInfo(props, propList);
    }
    
    public UUID compileCreatePropertySetStmt(final CreatePropertySetStmt stmt) throws MetaDataRepositoryException {
        final Map<String, Object> props = this.combineProperties(stmt.props);
        return this.ctx.putPropertySet(stmt.doReplace, stmt.name, props).uuid;
    }
    
    public UUID compileCreatePropertyVariableStmt(final CreatePropertyVariableStmt stmt) throws MetaDataRepositoryException {
        final Map<String, Object> props = this.combineProperties(stmt.props);
        return this.ctx.putPropertyVariable(stmt.doReplace, stmt.name, props).uuid;
    }
    
    private Map<String, Object> encryptSensitiveInfo(final Map<String, Object> props, final List<Property> propList) throws SecurityException, MetaDataRepositoryException {
        if (props == null || props.isEmpty()) {
            return props;
        }
        final Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
        temp.putAll(props);
        for (final String key : temp.keySet()) {
            final String key_flag = key + "_encrypted";
            if (props.get(key_flag) != null && !Utility.isValueEncryptionFlagSetToTrue(key, props)) {
                final String val = (String)props.get(key);
                props.put(key, Password.getEncryptedStatic(val));
                props.put(key_flag, true);
                for (int ik = 0; ik < propList.size(); ++ik) {
                    if (propList.get(ik).name.equalsIgnoreCase(key)) {
                        propList.set(ik, new Property(key, Password.getEncryptedStatic(val)));
                    }
                    if (propList.get(ik).name.equalsIgnoreCase(key_flag)) {
                        propList.set(ik, new Property(key_flag, true));
                    }
                }
            }
        }
        return props;
    }
    
    public Object compileCreateCacheStmt(final CreateCacheStmt stmt) throws MetaDataRepositoryException {
        final String adapterTypeName = stmt.src.getAdapterTypeName();
        final String adapterVersion = stmt.src.getVersion();
        final List<Property> handler_props = stmt.src.getProps();
        final String parse_handler = (stmt.parser != null) ? stmt.parser.getAdapterTypeName() : null;
        final String parserVersion = (stmt.parser != null && stmt.parser.getVersion() != null) ? stmt.parser.getVersion() : null;
        final List<Property> parse_handler_props = (stmt.parser != null) ? stmt.parser.getProps() : Collections.emptyList();
        final Map<String, Object> reader_props = this.combineProperties(handler_props);
        final Map<String, Object> parser_props = this.combineProperties(parse_handler_props);
        final Map<String, Object> query_props = this.combineProperties(stmt.query_props);
        final MetaInfo.Type type = this.ctx.getTypeInCurSchema(stmt.typename);
        if (type == null) {
            this.error("The specified type does not exist.", stmt.typename);
        }
        final MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(stmt.src.getAdapterTypeName(), adapterVersion);
        EntityType et = null;
        if (pt == null) {
            et = EntityType.forObject(stmt.src.getAdapterTypeName());
            if (et == null) {
                this.error("cannot load adapter", stmt.src.getAdapterTypeName());
            }
        }
        MetaInfo.MetaObject obj = null;
        if (pt != null && pt.adapterType != AdapterType.source && pt.adapterType != AdapterType.internal) {
            this.error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for caches.", stmt.src.getAdapterTypeName());
        }
        else if (et != null) {
            if (query_props.containsKey("refreshinterval") && query_props.containsKey("publishonrefresh")) {
                this.error("Refresh interval and publish on refresh won't work with EventTable", stmt.query_props);
            }
            if (!reader_props.containsKey("NAME")) {
                this.error("No input stream name specified for EventTable", stmt.src.getProps());
            }
            obj = this.ctx.get((String)reader_props.get("NAME"), et);
            if (obj == null) {
                final String streamName = (String)reader_props.get("NAME");
                obj = this.ctx.putStream(false, this.makeObjectName(streamName), type.getUuid(), null, null, null, null, null);
            }
        }
        if (pt != null) {
            this.validateAdapterPropsAndValues(pt, reader_props, handler_props);
            MetaInfo.PropertyTemplateInfo parserPropertyTemplate = null;
            if (parse_handler != null) {
                parserPropertyTemplate = this.ctx.getAdapterProperties(parse_handler, parserVersion);
            }
            if (parse_handler != null) {
                if (parserPropertyTemplate == null) {
                    this.error("No such parser exists.", stmt.parser.getAdapterTypeName());
                }
                parser_props.put("handler", parserPropertyTemplate.className);
            }
            if (pt.requiresParser && parserPropertyTemplate == null) {
                this.error(stmt.src.getAdapterTypeName() + " requires parser.", stmt.src.getAdapterTypeName());
            }
            if (parserPropertyTemplate != null) {
                this.validateAdapterPropsAndValues(parserPropertyTemplate, parser_props, null);
            }
        }
        final MetaInfo.Cache cacheObject = this.ctx.putCache(stmt.doReplace, this.makeObjectName(stmt.name), (pt == null) ? null : pt.className, reader_props, parser_props, query_props, type.uuid, (pt == null) ? null : pt.getInputClass());
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from type " + type.name + " => cache " + cacheObject.name + " } "));
        }
        this.addDependency(type, cacheObject);
        this.addCurrentApplicationFlowDependencies(type);
        this.addCurrentApplicationFlowDependencies(cacheObject);
        stmt.sourceText = Utility.createCacheStatementText(stmt.name, stmt.doReplace, adapterTypeName, handler_props, parse_handler, parse_handler_props, stmt.query_props, stmt.typename);
        this.addSourceTextToMetaObject(stmt, cacheObject);
        return null;
    }
    
    private void addDependency(final MetaInfo.MetaObject fromObject, final MetaInfo.MetaObject toObject) throws MetaDataRepositoryException {
        if (fromObject != null && toObject != null) {
            fromObject.addReverseIndexObjectDependencies(toObject.uuid);
            this.metadata_repo.updateMetaObject(fromObject, this.ctx.getSessionID());
        }
        else {
            Compiler.logger.warn((Object)("Can't add dependency between " + fromObject + " and " + toObject));
        }
    }
    
    private MetaInfo.Type getTypeForCLass(final Class<?> clazz) throws MetaDataRepositoryException {
        MetaInfo.Type type = null;
        if (!clazz.equals(NotSet.class)) {
            type = this.ctx.getType("Global." + clazz.getSimpleName());
            if (type != null) {
                return type;
            }
            final String typeName = this.ctx.addSchemaPrefix(null, clazz.getSimpleName());
            type = this.ctx.getType(typeName);
            if (type == null) {
                final Map<String, String> fields = new LinkedHashMap<String, String>();
                final Field[] declaredFields;
                final Field[] cFields = declaredFields = clazz.getDeclaredFields();
                for (final Field f : declaredFields) {
                    if (javassist.Modifier.isPublic(f.getModifiers())) {
                        fields.put(f.getName(), f.getType().getCanonicalName());
                    }
                }
                type = new MetaInfo.Type();
                type.construct(typeName, this.ctx.getNamespace(null), clazz.getName(), fields, null, false);
                this.ctx.putType(type);
                type = this.ctx.getType(typeName);
            }
        }
        return type;
    }
    
    private void validateAdapterPropsAndValues(final MetaInfo.PropertyTemplateInfo pt, final Map<String, Object> props, final List<Property> propList) {
        final Map<String, MetaInfo.PropertyDef> tps = pt.getPropertyMap();
        for (final Map.Entry<String, MetaInfo.PropertyDef> tp : tps.entrySet()) {
            final String pname = tp.getKey();
            final MetaInfo.PropertyDef def = tp.getValue();
            final Class<?> ptype = def.type;
            final boolean preq = def.required;
            final String defVal = def.defaultValue;
            Object val = props.get(pname);
            if (val == null) {
                if (preq) {
                    this.error("property " + pname + " is required, but is not specified", null);
                }
                val = this.castDefaultValueToProperType(pname, defVal, ptype, props, null);
                props.put(pname, val);
            }
            else {
                if (!(val instanceof String) || ptype.equals(String.class)) {
                    continue;
                }
                val = this.castDefaultValueToProperType(pname, (String)val, ptype, props, propList);
                props.put(pname, val);
            }
        }
    }
    
    private void checkCompoundStreamName(final String name) throws MetaDataRepositoryException {
        if (name.indexOf(46) >= 0) {
            final String[] names = name.split("\\.");
            if (this.ctx.getNamespace(names[0]) == null) {
                this.error("cannot find namespace <" + names[0] + ">", name);
            }
        }
    }
    
    private void addChannelNameForSubscriptions(final CreateSourceOrTargetStmt stmt) {
        if (stmt.srcOrDest.getAdapterTypeName().equalsIgnoreCase("WebAlertAdapter")) {
            final List<Property> handler_props = stmt.srcOrDest.getProps();
            boolean add = false;
            boolean channelNameGiven = false;
            for (final Property property : handler_props) {
                if (property.name.equalsIgnoreCase("isSubscription")) {
                    add = true;
                }
                if (property.name.equalsIgnoreCase("channelName") && property.value != null) {
                    if (((String)property.value).isEmpty()) {
                        continue;
                    }
                    channelNameGiven = true;
                }
            }
            if (add && !channelNameGiven) {
                final Property p = new Property("channelName", this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)) + "_" + stmt.name);
                stmt.srcOrDest.getProps().add(p);
            }
        }
    }
    
    void revalidateTheComponents(final MetaInfo.CQ cq, final boolean doReplace) throws MetaDataRepositoryException {
        final UUID streamUUID = cq.stream;
        final MetaInfo.Stream stream = (MetaInfo.Stream)this.ctx.getObject(streamUUID);
        final UUID typeUUID = stream.dataType;
        final MetaInfo.Type type = (MetaInfo.Type)this.ctx.getObject(typeUUID);
        final List<TypeField> makeFieldList = new ArrayList<TypeField>();
        for (final Map.Entry<String, String> entry : type.fields.entrySet()) {
            final String fieldName = entry.getKey();
            final String fieldType = entry.getValue();
            final boolean contains = type.keyFields.contains(fieldName);
            makeFieldList.add(new TypeField(fieldName, new TypeName(fieldType, 0), contains));
        }
        final String typeString = Utility.createTypeStatementText(type.getName(), doReplace, makeFieldList);
        type.setSourceText(typeString);
        this.ctx.updateMetaObject(type);
        String[] partArray = null;
        if (stream.getPartitioningFields() != null) {
            partArray = stream.getPartitioningFields().toArray(new String[0]);
        }
        final String streamString = Utility.createStreamStatementText(stream.getName(), doReplace, type.getName(), partArray);
        stream.setSourceText(streamString);
        this.ctx.updateMetaObject(stream);
    }
    
    void revalidateTheComponents(final CreateSourceOrTargetStmt stmt) {
        try {
            final MetaInfo.Source source = this.ctx.getSourceInCurSchema(stmt.name);
            final UUID uuidOfSourceStream = source.getOutputStream();
            final MetaInfo.Stream st = (MetaInfo.Stream)this.ctx.getObject(uuidOfSourceStream);
            MetaInfo.Type typeOfStr = (MetaInfo.Type)this.ctx.getObject(st.dataType);
            final boolean debug = false;
            if (debug) {
                System.out.println();
                System.out.println("1) Revalidating the source " + stmt.name);
                System.out.println("2) stream " + st.name);
                System.out.println("3) type " + typeOfStr + " >> " + st.dataType);
            }
            if (typeOfStr == null) {
                final String parserOrFormatterHandler = (stmt.parserOrFormatter != null) ? stmt.parserOrFormatter.getAdapterTypeName() : null;
                final String parserorFormatterVersion = (stmt.parserOrFormatter != null) ? stmt.parserOrFormatter.getVersion() : null;
                final String adapterTypeName = stmt.srcOrDest.getAdapterTypeName();
                final String adapterVersion = stmt.srcOrDest.getVersion();
                final MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(adapterTypeName, adapterVersion);
                MetaInfo.PropertyTemplateInfo parserOrFormatterPropertyTemplate = null;
                if (parserOrFormatterHandler != null) {
                    parserOrFormatterPropertyTemplate = this.ctx.getAdapterProperties(parserOrFormatterHandler, parserorFormatterVersion);
                    if (parserOrFormatterPropertyTemplate == null) {
                        this.error("cannot find parser template", parserOrFormatterHandler);
                    }
                    if (stmt.what == EntityType.SOURCE && parserOrFormatterPropertyTemplate.adapterType != AdapterType.parser && parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal) {
                        this.error("The specified parser is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct parser adapter for sources.", adapterTypeName);
                    }
                    if (stmt.what == EntityType.TARGET && parserOrFormatterPropertyTemplate.adapterType != AdapterType.formatter && parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal) {
                        this.error("The specified formatter is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct formatter adapter for targets.", adapterTypeName);
                    }
                }
                final Class<?> inputTypeClass = (parserOrFormatterPropertyTemplate == null) ? pt.getInputClass() : parserOrFormatterPropertyTemplate.getInputClass();
                final Map<String, String> fields = new LinkedHashMap<String, String>();
                final Field[] cFields = inputTypeClass.getDeclaredFields();
                final String typeName = this.ctx.addSchemaPrefix(null, inputTypeClass.getSimpleName());
                for (final Field f : cFields) {
                    if (javassist.Modifier.isPublic(f.getModifiers())) {
                        fields.put(f.getName(), f.getType().getCanonicalName());
                    }
                }
                final MetaInfo.Type type = new MetaInfo.Type();
                type.construct(typeName, this.ctx.getNamespace(null), inputTypeClass.getName(), fields, null, false);
                type.setUuid(st.dataType);
                this.ctx.putType(type);
                typeOfStr = (MetaInfo.Type)this.ctx.getObject(st.dataType);
                if (debug) {
                    System.out.println("Expected " + inputTypeClass);
                    System.out.println("Again 4) type " + typeOfStr + " >> " + st.dataType);
                    System.out.println();
                }
            }
        }
        catch (MetaDataRepositoryException e) {
            e.printStackTrace();
        }
    }
    
    public MetaInfo.MetaObject compileCreateSourceOrTargetStmt(final CreateSourceOrTargetStmt stmt) throws MetaDataRepositoryException {
        if (ServerUpgradeUtility.isUpgrading) {
            this.revalidateTheComponents(stmt);
            final MetaInfo.Source source = this.ctx.getSourceInCurSchema(stmt.name);
            source.getMetaInfoStatus().setValid(true);
            this.ctx.updateMetaObject(source);
            this.ctx.addToFlow(source);
            return source;
        }
        final String adapterTypeName = stmt.srcOrDest.getAdapterTypeName();
        final String adapterVersion = stmt.srcOrDest.getVersion();
        this.addChannelNameForSubscriptions(stmt);
        final List<Property> handler_props = stmt.srcOrDest.getProps();
        final String parserOrFormatterHandler = (stmt.parserOrFormatter != null) ? stmt.parserOrFormatter.getAdapterTypeName() : null;
        final List<Property> parserOrFormatterProps = (stmt.parserOrFormatter != null) ? stmt.parserOrFormatter.getProps() : Collections.emptyList();
        final String parserorFormatterVersion = (stmt.parserOrFormatter != null) ? stmt.parserOrFormatter.getVersion() : null;
        if (Compiler.logger.isTraceEnabled()) {
            Compiler.logger.trace((Object)("Trying to load adapter '" + adapterTypeName + "' of version '" + adapterVersion + "'"));
        }
        final MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(adapterTypeName, adapterVersion);
        if (pt == null) {
            this.error("cannot load adapter", adapterTypeName);
        }
        if ((pt.requiresParser || pt.requiresFormatter) && parserOrFormatterHandler == null) {
            this.error(stmt.srcOrDest.getAdapterTypeName() + " requires parser.", stmt.srcOrDest.getAdapterTypeName());
        }
        if (stmt.what == EntityType.SOURCE && pt.adapterType != AdapterType.source && pt.adapterType != AdapterType.internal) {
            this.error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for sources.", adapterTypeName);
        }
        if (stmt.what == EntityType.TARGET && pt.adapterType != AdapterType.target && pt.adapterType != AdapterType.internal) {
            this.error("The specified adapter is a " + pt.adapterType + " adapter, please use a target adapter for targets.", adapterTypeName);
        }
        MetaInfo.PropertyTemplateInfo parserOrFormatterPropertyTemplate = null;
        if (parserOrFormatterHandler != null) {
            parserOrFormatterPropertyTemplate = this.ctx.getAdapterProperties(parserOrFormatterHandler, parserorFormatterVersion);
            if (parserOrFormatterPropertyTemplate == null) {
                this.error("cannot find parser template", parserOrFormatterHandler);
            }
            if (stmt.what == EntityType.SOURCE && parserOrFormatterPropertyTemplate.adapterType != AdapterType.parser && parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal) {
                this.error("The specified parser is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct parser adapter for sources.", adapterTypeName);
            }
            if (stmt.what == EntityType.TARGET && parserOrFormatterPropertyTemplate.adapterType != AdapterType.formatter && parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal) {
                this.error("The specified formatter is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct formatter adapter for targets.", adapterTypeName);
            }
        }
        final Map<String, Object> props = this.combineProperties(handler_props);
        final Map<String, Object> parserOrFormatterProperties = this.combineProperties(parserOrFormatterProps);
        this.validateAdapterPropsAndValues(pt, props, handler_props);
        if (props != null) {
            props.put("adapterName", pt.name);
        }
        if (parserOrFormatterPropertyTemplate != null) {
            this.validateAdapterPropsAndValues(parserOrFormatterPropertyTemplate, parserOrFormatterProperties, null);
            parserOrFormatterProperties.put("handler", parserOrFormatterPropertyTemplate.className);
            if (stmt.what == EntityType.SOURCE) {
                parserOrFormatterProperties.put("parserName", parserOrFormatterPropertyTemplate.name);
            }
            else {
                parserOrFormatterProperties.put("formatterName", parserOrFormatterPropertyTemplate.name);
            }
        }
        final Class<?> inputTypeClass = (parserOrFormatterPropertyTemplate == null) ? pt.getInputClass() : parserOrFormatterPropertyTemplate.getInputClass();
        final MetaInfo.Type inputType = this.getTypeForCLass(inputTypeClass);
        final Class<?> outputTypeClass = pt.getOutputClass();
        final MetaInfo.Type outputType = this.getTypeForCLass(outputTypeClass);
        assert stmt.what == EntityType.TARGET;
        final UUID resType = (stmt.what == EntityType.SOURCE) ? inputType.uuid : outputType.uuid;
        UUID oldSourceUUID = null;
        UUID oldTargetUUID = null;
        UUID oldStreamUUID = null;
        if (stmt.doReplace) {
            if (this.ctx.getSourceInCurSchema(stmt.name) != null || this.ctx.getTargetInCurSchema(stmt.name) != null) {
                if (this.ctx.recompileMode) {
                    if (stmt.what == EntityType.SOURCE) {
                        final MetaInfo.Source sourceMetaObject = this.ctx.getSourceInCurSchema(stmt.name);
                        oldSourceUUID = sourceMetaObject.uuid;
                        if (this.ctx.getStreamInCurSchema(stmt.getStreamName()).getMetaInfoStatus().isGenerated()) {
                            oldStreamUUID = this.ctx.getStreamInCurSchema(stmt.getStreamName()).uuid;
                            this.ctx.removeObject(this.ctx.getStreamInCurSchema(stmt.getStreamName()));
                        }
                        this.ctx.removeObject(sourceMetaObject);
                    }
                    if (stmt.what == EntityType.TARGET) {
                        final MetaInfo.Target targetMetaObject = this.ctx.getTargetInCurSchema(stmt.name);
                        oldTargetUUID = targetMetaObject.uuid;
                        final MetaInfo.Stream streamMetaObject = this.ctx.getStreamInCurSchema(stmt.getStreamName());
                        if (!streamMetaObject.getMetaInfoStatus().isGenerated()) {
                            oldStreamUUID = this.ctx.getStreamInCurSchema(stmt.getStreamName()).uuid;
                            this.ctx.removeObject(this.ctx.getStreamInCurSchema(stmt.getStreamName()));
                        }
                        this.ctx.removeObject(targetMetaObject);
                    }
                }
                else if (stmt.what == EntityType.SOURCE) {
                    DropMetaObject.checkPermissionToDrop(this.ctx.getSourceInCurSchema(stmt.name), this.ctx.getSessionID());
                    DropMetaObject.DropSource.drop(this.ctx, this.ctx.getSourceInCurSchema(stmt.name), DropMetaObject.DropRule.NONE, this.ctx.getSessionID());
                }
                else {
                    DropMetaObject.checkPermissionToDrop(this.ctx.getTargetInCurSchema(stmt.name), this.ctx.getSessionID());
                    DropMetaObject.DropTarget.drop(this.ctx, this.ctx.getTargetInCurSchema(stmt.name), DropMetaObject.DropRule.NONE, this.ctx.getSessionID());
                }
            }
        }
        MetaInfo.MetaObject stream = this.ctx.getDataSourceInCurSchema(stmt.getStreamName());
        if (stream == null) {
            this.checkCompoundStreamName(stmt.getStreamName());
            this.checkPartitionFields(stmt.getPartitionFields(), resType, stmt.getStreamName());
            if (this.ctx.recompileMode && oldStreamUUID != null) {
                stream = this.ctx.putStream(false, this.makeObjectName(stmt.getStreamName()), resType, stmt.getPartitionFields(), null, null, oldStreamUUID, null);
            }
            else {
                stream = this.ctx.putStream(false, this.makeObjectName(stmt.getStreamName()), resType, stmt.getPartitionFields(), null, null, null, null);
            }
            stream.getMetaInfoStatus().setGenerated(true);
            this.metadata_repo.updateMetaObject(stream, this.ctx.getSessionID());
        }
        Map<String, Object> parallelismConfig = null;
        if (pt.name.equalsIgnoreCase("KafkaWriter") || pt.name.equalsIgnoreCase("S3Writer")) {
            final Object parallelThreads = props.get("ParallelThreads");
            final Object partKey = props.get("PartitionKey");
            if (parallelThreads != null) {
                if ((int)parallelThreads <= 0) {
                    throw new CompilationException("ParallelThreads should be configured with a value greater than 0");
                }
                if (partKey == null || ((String)partKey).isEmpty()) {
                    if (((MetaInfo.Stream)stream).partitioningFields == null || ((MetaInfo.Stream)stream).partitioningFields.size() == 0) {
                        throw new CompilationException("PartitionKey is required if ParallelThreads is configured.");
                    }
                    parallelismConfig = new HashMap<String, Object>();
                    parallelismConfig.put("parallelismFactor", props.get("ParallelThreads"));
                    parallelismConfig.put("parallelismKey", ((MetaInfo.Stream)stream).partitioningFields.get(0));
                }
                else {
                    parallelismConfig = new HashMap<String, Object>();
                    parallelismConfig.put("parallelismFactor", props.get("ParallelThreads"));
                    parallelismConfig.put("parallelismKey", props.get("PartitionKey"));
                }
            }
        }
        MetaInfo.MetaObject metaObject = null;
        if (stmt.what == EntityType.SOURCE) {
            metaObject = this.ctx.putSource(stmt.doReplace, this.makeObjectName(stmt.name), pt.className, props, parserOrFormatterProperties, stream.uuid, oldSourceUUID);
        }
        else {
            metaObject = this.ctx.putTarget(stmt.doReplace, this.makeObjectName(stmt.name), pt.className, props, parserOrFormatterProperties, parallelismConfig, stream.uuid, oldTargetUUID);
        }
        final MetaInfo.Type type = (stmt.what == EntityType.SOURCE) ? inputType : outputType;
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from stream " + stream.name + " => source/target " + metaObject.name + " } "));
        }
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from type " + type.name + " => source/target " + metaObject.name + " } "));
        }
        this.addDependency(type, stream);
        this.addDependency(stream, metaObject);
        this.addCurrentApplicationFlowDependencies(metaObject);
        this.addCurrentApplicationFlowDependencies(stream);
        if (stmt.what == EntityType.TARGET) {
            if (((MetaInfo.Target)metaObject).isSubscription()) {
                stmt.sourceText = Utility.createSubscriptionStatementText(stmt.name, stmt.doReplace, adapterTypeName, handler_props, parserOrFormatterHandler, parserOrFormatterProps, stmt.getStreamName());
            }
            else {
                stmt.sourceText = Utility.createTargetStatementText(stmt.name, stmt.doReplace, adapterTypeName, adapterVersion, handler_props, parserOrFormatterHandler, parserorFormatterVersion, parserOrFormatterProps, stmt.getStreamName());
            }
        }
        else if (stmt.what == EntityType.SOURCE) {
            stmt.sourceText = Utility.createSourceStatementText(stmt.name, stmt.doReplace, adapterTypeName, adapterVersion, handler_props, parserOrFormatterHandler, parserorFormatterVersion, parserOrFormatterProps, stmt.getStreamName(), stmt.getGeneratedStream());
        }
        this.addSourceTextToMetaObject(stmt, metaObject);
        return metaObject;
    }
    
    private Object castDefaultValueToProperType(final String key, String value, Class<?> type, final Map<String, Object> props, final List<Property> propList) {
        Object correctType = null;
        if (value == null || value.isEmpty()) {
            return null;
        }
        if (!value.startsWith("$")) {
            if (type == Character.class) {
                correctType = value.charAt(0);
            }
            else if (type == Boolean.class) {
                if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes")) {
                    correctType = true;
                }
                else {
                    correctType = false;
                }
            }
            else if (type == Password.class) {
                correctType = new Password();
                if (Utility.isValueEncryptionFlagSetToTrue(key, props)) {
                    try {
                        ((Password)correctType).setEncrypted(value);
                        return correctType;
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException("Expecting encrypted " + key + ", or set " + key + "_encrypted to false in " + ((props.get("adapterName") != null) ? props.get("adapterName").toString() : "TQL"));
                    }
                }
                ((Password)correctType).setPlain(value);
                this.updateMapAndListWithEncryptionFlag(key, value, props, propList);
            }
            else if (type == Enum.class || type.isEnum()) {
                List<String> list = null;
                try {
                    list = (List<String>)StringUtils.getEnumValuesAsList(type.getName());
                    if (list != null && list.size() > 0) {
                        for (final String str : list) {
                            if (str.equalsIgnoreCase(value)) {
                                value = str;
                                break;
                            }
                        }
                    }
                    final Class enumClass = Class.forName(type.getName());
                    correctType = Enum.valueOf(enumClass, value);
                }
                catch (Exception ex) {
                    final String allowedVals = StringUtils.getListValuesAsString((List)list, ", ");
                    final String errmsg = "Error setting field : " + key + ". '" + value + "' is wrong value. Check for empty spaces too. Allowed values are : " + allowedVals;
                    Compiler.logger.error((Object)errmsg, (Throwable)ex);
                    throw new IllegalArgumentException(errmsg);
                }
            }
            else {
                try {
                    if (type.isPrimitive()) {
                        type = CompilerUtils.getBoxingType(type);
                    }
                    correctType = type.getConstructor(String.class).newInstance(value);
                }
                catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex3) {
                    Compiler.logger.error(ex3.getMessage());
                }
            }
            return correctType;
        }
        if (type == Password.class) {
            return new Password(value);
        }
        return value;
    }
    
    private void updateMapAndListWithEncryptionFlag(final String key, final String value, final Map<String, Object> props, final List<Property> propList) {
        boolean update = false;
        boolean add = false;
        if (propList != null && propList.size() > 0) {
            for (int ik = 0; ik < propList.size(); ++ik) {
                if (propList.get(ik).name.equalsIgnoreCase(key)) {
                    propList.set(ik, new Property(key, Password.getEncryptedStatic(value)));
                    if (props.get(key + "_encrypted") == null) {
                        add = true;
                    }
                    else {
                        update = true;
                    }
                }
            }
            if (add) {
                propList.add(new Property(key + "_encrypted", true));
            }
            if (update) {
                for (int ik = 0; ik < propList.size(); ++ik) {
                    if (propList.get(ik).name.equalsIgnoreCase(key + "_encrypted")) {
                        propList.set(ik, new Property(key + "_encrypted", true));
                    }
                }
            }
        }
    }
    
    public Object compileCreateAppOrFlowStatement(final CreateAppOrFlowStatement stmt) throws MetaDataRepositoryException {
        Map<EntityType, LinkedHashSet<UUID>> objects = null;
        if (stmt.entities != null) {
            objects = Factory.makeMap();
            for (final Pair<EntityType, String> p : stmt.entities) {
                final String name = this.ctx.addSchemaPrefix(null, p.second);
                final MetaInfo.MetaObject obj = this.ctx.get(name, p.first);
                if (obj == null) {
                    this.error(p.first.name().toLowerCase() + " [" + p.second + "]does not exist", p);
                }
                else {
                    LinkedHashSet<UUID> list = objects.get(obj.type);
                    if (list == null) {
                        list = new LinkedHashSet<UUID>();
                        objects.put(obj.type, list);
                    }
                    list.add(obj.uuid);
                }
            }
        }
        Map<String, Object> ehprops = null;
        if (stmt.eh != null) {
            ehprops = this.combineProperties(stmt.eh.props);
            if (!ExceptionType.validate(ehprops)) {
                Compiler.logger.error((Object)"validation of exceptionhandler details failed. enter proper values and recompile applicaiton");
                throw new CompilationException("validation of exceptionhandler details failed. enter proper values and recompile applicaiton");
            }
        }
        final MetaInfo.Flow flowOrApp = this.ctx.putFlow(stmt.what, stmt.doReplace, this.makeObjectName(stmt.name), stmt.encrypted, stmt.recoveryDesc, ehprops, objects, new HashSet<String>());
        final Set<Map.Entry<EntityType, LinkedHashSet<UUID>>> objectsByType = flowOrApp.getObjects().entrySet();
        if (objectsByType != null && !objectsByType.isEmpty()) {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> entry : objectsByType) {
                for (final UUID uuid : entry.getValue()) {
                    final MetaInfo.MetaObject objectInFlow = this.ctx.getObject(uuid);
                    this.addDependency(objectInFlow, flowOrApp);
                }
            }
        }
        if (stmt.what == EntityType.FLOW) {
            if (Compiler.logger.isDebugEnabled()) {
                Compiler.logger.debug((Object)("Added dependency: { from flow " + stmt.name + " => app " + this.ctx.getCurApp().name + " } "));
            }
            if (this.ctx.getCurApp() != null) {
                this.addDependency(flowOrApp, this.ctx.getCurApp());
            }
            else {
                Compiler.logger.warn((Object)("Trying to add dependency between a Flow:" + flowOrApp + " and App: Null"));
            }
        }
        return null;
    }
    
    public Object compileCreateDeploymentGroupStmt(final CreateDeploymentGroupStmt stmt) throws MetaDataRepositoryException {
        this.ctx.putDeploymentGroup(stmt.name, stmt.deploymentGroup, stmt.minServers, stmt.maxApps);
        return null;
    }
    
    public Object compileAlterDeploymentGroupStmt(final AlterDeploymentGroupStmt stmt) throws MetaDataRepositoryException {
        this.ctx.alterDeploymentGroup(stmt.groupname, stmt.nodesAdded, stmt.nodesRemoved, stmt.minServers, stmt.limiApplications);
        return null;
    }
    
    public static boolean compareKeyFields(final List<String> k1, final List<String> k2) {
        return k1.equals(k2);
    }
    
    public Object compileCreateWASStmt(final CreateWASStmt stmt) throws MetaDataRepositoryException {
        final TypeDefOrName typeDef = stmt.typeDef;
        UUID typeid;
        if (typeDef.typeName != null) {
            assert typeDef.typeDef == null;
            final MetaInfo.MetaObject type = this.ctx.getTypeInCurSchema(typeDef.typeName);
            if (type == null) {
                this.error("no such type declaration", typeDef.typeName);
            }
            typeid = type.uuid;
        }
        else {
            assert typeDef.typeDef != null;
            typeid = this.createType(false, RuntimeUtils.genRandomName("type"), typeDef.typeDef, true);
        }
        final List<EventType> eventTypes = stmt.evenTypes;
        final List<UUID> eventTypeIDs = new ArrayList<UUID>();
        final List<String> eventTypeKeys = new ArrayList<String>();
        final List<MetaInfo.Type> eventTypesList = new ArrayList<MetaInfo.Type>();
        final Map<String, List<String>> ets = new HashMap<String, List<String>>();
        for (final EventType et : eventTypes) {
            final MetaInfo.Type type2 = this.ctx.getTypeInCurSchema(et.typeName);
            if (type2 == null) {
                this.error("no such event type declared", et.typeName);
            }
            eventTypeIDs.add(type2.uuid);
            eventTypeKeys.add(et.keyFields.get(0));
            eventTypesList.add(type2);
            ets.put(et.typeName, et.keyFields);
        }
        final Interval how = stmt.howToPersist;
        final Map<String, Object> props = this.combineProperties(stmt.properties);
        final MetaInfo.HDStore hdStroreObject = this.ctx.putHDStore(stmt.doReplace, this.makeObjectName(stmt.name), typeid, how, eventTypeIDs, eventTypeKeys, props);
        final MetaInfo.Type contextTypeMetaObject = (MetaInfo.Type)this.ctx.getObject(typeid);
        if (Compiler.logger.isDebugEnabled()) {
            Compiler.logger.debug((Object)("Added dependency: { from type " + contextTypeMetaObject.name + " => stream " + hdStroreObject.name + " } "));
        }
        this.addDependency(contextTypeMetaObject, hdStroreObject);
        this.addCurrentApplicationFlowDependencies(contextTypeMetaObject);
        if (eventTypesList != null) {
            for (final MetaInfo.Type eventType : eventTypesList) {
                if (Compiler.logger.isDebugEnabled()) {
                    Compiler.logger.debug((Object)("Added dependency: { from type " + eventType.name + " => stream " + hdStroreObject.name + " } "));
                }
                this.addDependency(eventType, hdStroreObject);
                this.addCurrentApplicationFlowDependencies(eventType);
            }
        }
        this.addCurrentApplicationFlowDependencies(hdStroreObject);
        final HStorePersistencePolicy policy = new HStorePersistencePolicy(stmt.howToPersist, stmt.properties);
        String ival = null;
        if (how != null) {
            ival = String.valueOf(how.value / 1000000L) + " SECOND ";
        }
        stmt.sourceText = Utility.createWASStatementText(stmt.name, stmt.doReplace, stmt.typeDef.typeName, ets, ival, policy);
        this.addSourceTextToMetaObject(stmt, hdStroreObject);
        return null;
    }
    
    public Object compileCreateNamespaceStatement(final CreateNamespaceStatement stmt) throws MetaDataRepositoryException {
        this.ctx.putNamespace(stmt.doReplace, stmt.name);
        return null;
    }
    
    public Object compileCreateUserStmt(final CreateUserStmt stmt) {
        if (stmt.prop.lroles != null) {
            for (final String rname : stmt.prop.lroles) {
                final Pattern pattern = Pattern.compile("\\s");
                final Matcher matcher = pattern.matcher(rname);
                final boolean found = matcher.find();
                if (found) {
                    this.error("Space in the ROLE NAME. ", stmt.prop.lroles);
                }
            }
        }
        if (stmt.prop.ldap != null) {
            final AuthLayer al = LDAPAuthLayer.get(Utility.convertToFullQualifiedName(this.ctx.getCurNamespace(), stmt.prop.ldap));
            if (al == null) {
                this.error("No LDAP propertyset exists with name", stmt.prop.ldap);
                return null;
            }
            String name = stmt.prop.getAlias();
            if (name == null) {
                name = stmt.name;
            }
            try {
                if (!al.find(name)) {
                    this.error("No user found in LDAP server with name: " + name, name);
                    return null;
                }
            }
            catch (DatallRuntimeException strex) {
                this.error(strex.getMessage(), name);
                return null;
            }
        }
        try {
            this.ctx.putUser(stmt.name, stmt.password, stmt.prop);
        }
        catch (Exception e) {
            throw new Warning(e.getMessage());
        }
        return null;
    }
    
    public Object compileCreateRoleStmt(final CreateRoleStmt stmt) {
        final Pattern pattern = Pattern.compile("\\s");
        final Matcher matcher = pattern.matcher(stmt.name);
        final boolean found = matcher.find();
        if (stmt.name.split(":").length != 2) {
            this.error("Rolename has wrong format ", stmt.name);
        }
        if (found) {
            this.error("Space in the ROLE NAME.", stmt.name);
        }
        try {
            this.ctx.putRole(stmt.name);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return null;
    }
    
    public Object compileConnectStmt(final ConnectStmt stmt) throws MetaDataRepositoryException {
        this.ctx.Connect(stmt.name, stmt.password, stmt.clusterid, stmt.host);
        return null;
    }
    
    public Object compileImportDataStmt(final ImportDataStmt stmt) {
        if ("METADATA".equalsIgnoreCase(stmt.what)) {
            try {
                final File f = new File(stmt.where);
                final FileReader fr = new FileReader(f);
                final StringBuilder jsonBuilder = new StringBuilder();
                final char[] cbuf = new char[256];
                for (int result = fr.read(cbuf); result != -1; result = fr.read(cbuf)) {
                    jsonBuilder.append(cbuf, 0, result);
                }
                final String json = jsonBuilder.toString();
                this.metadata_repo.importMetadataFromJson(json, stmt.replace);
                fr.close();
            }
            catch (IOException e) {
                Compiler.logger.error((Object)e);
            }
        }
        else {
            Compiler.logger.info((Object)("Cannot export data becasue of unknown datatype: " + stmt.what));
        }
        return null;
    }
    
    public Object compileExportDataStmt(final ExportDataStmt stmt) {
        if ("METADATA".equalsIgnoreCase(stmt.what)) {
            if (stmt.where == null || stmt.where.equals("STDOUT")) {
                final String json = this.metadata_repo.exportMetadataAsJson();
                System.out.print(json);
            }
            else {
                try {
                    final String json = this.metadata_repo.exportMetadataAsJson();
                    final File f = new File(stmt.where);
                    final FileWriter fw = new FileWriter(f);
                    fw.write(json);
                    fw.close();
                }
                catch (IOException e) {
                    Compiler.logger.error((Object)e.getMessage(), (Throwable)e);
                }
            }
        }
        else {
            Compiler.logger.info((Object)("Cannot export data becasue of unknown datatype: " + stmt.what));
        }
        return null;
    }
    
    public Object compileSetStmt(final SetStmt stmt) {
        if (stmt.paramname == null && stmt.paramvalue == null) {
            Tungsten.listLogLevels();
        }
        else if (stmt.paramname.equalsIgnoreCase("PRINTFORMAT")) {
            if (stmt.paramvalue instanceof String) {
                if (stmt.paramvalue.toString().equalsIgnoreCase("JSON")) {
                    Tungsten.currentFormat = Tungsten.PrintFormat.JSON;
                }
                else if (stmt.paramvalue.toString().equalsIgnoreCase("ROW_FORMAT")) {
                    Tungsten.currentFormat = Tungsten.PrintFormat.ROW_FORMAT;
                }
                else {
                    Tungsten.currentFormat = Tungsten.PrintFormat.JSON;
                }
            }
        }
        else {
            if (stmt.paramname.equalsIgnoreCase("LOGLEVEL")) {
                if (this.ctx == null || this.ctx.getSessionID() == null) {
                    return null;
                }
                try {
                    final MetaInfo.User user = HSecurityManager.get().getAuthenticatedUser(this.ctx.getSessionID());
                    final MetaInfo.Role rl = new MetaInfo.Role();
                    rl.construct(MetaInfo.GlobalNamespace, "admin");
                    if (user.getRoles().contains(rl)) {
                        Utility.changeLogLevel(stmt.paramvalue, true);
                        this.ctx.SetStmtRemoteCall(stmt.paramname, stmt.paramvalue);
                        return null;
                    }
                    throw new RuntimeException("No permissions to set log level for the user : " + user);
                }
                catch (MetaDataRepositoryException e) {
                    if (Compiler.logger.isInfoEnabled()) {
                        Compiler.logger.info((Object)e.getMessage());
                    }
                    throw new RuntimeException(e.getMessage());
                }
            }
            if (stmt.paramname.equalsIgnoreCase("MONITOR")) {
                final IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
                final Boolean newValue = new Boolean(stmt.paramvalue != null && stmt.paramvalue.toString().equalsIgnoreCase("TRUE"));
                if (!newValue.equals(clusterSettings.get((Object)"com.datasphere.config.enable-monitor"))) {
                    clusterSettings.put("com.datasphere.config.enable-monitor", newValue);
                }
            }
            else if (stmt.paramname.equalsIgnoreCase("MONITORPERSISTENCE")) {
                final IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
                final Boolean newValue = new Boolean(stmt.paramvalue != null && stmt.paramvalue.toString().equalsIgnoreCase("TRUE"));
                if (!newValue.equals(clusterSettings.get((Object)"com.datasphere.config.enable-monitor-persistence"))) {
                    clusterSettings.put("com.datasphere.config.enable-monitor-persistence", newValue);
                }
            }
            else if (stmt.paramname.equalsIgnoreCase("MONDBMAX")) {
                final IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
                final String noCommas = stmt.paramvalue.toString().replace(",", "");
                Integer newValue2 = null;
                try {
                    newValue2 = Integer.parseInt(noCommas);
                    if (!newValue2.equals(clusterSettings.get((Object)"com.datasphere.config.monitor-db-max"))) {
                        clusterSettings.put("com.datasphere.config.monitor-db-max", newValue2);
                    }
                }
                catch (NumberFormatException e2) {
                    System.out.println("Could not parse as integer: " + stmt.paramvalue);
                }
            }
            else if (stmt.paramname.equalsIgnoreCase("commandlog")) {
                if (!HazelcastSingleton.isClientMember()) {
                    return null;
                }
                if (stmt.paramvalue != null) {
                    final String val = stmt.paramvalue.toString();
                    if (val.equalsIgnoreCase("true")) {
                        Tungsten.setCommandLog(true);
                    }
                    else if (val.equalsIgnoreCase("false")) {
                        Tungsten.setCommandLog(false);
                    }
                    else {
                        System.out.println("Syntax : SET COMMANDLOG TRUE/FALSE");
                    }
                }
                else {
                    System.out.println("Syntax : SET COMMANDLOG TRUE/FALSE");
                }
            }
            else {
                Compiler.logger.warn((Object)("'SET' param name '" + stmt.paramname + "' not recognized"));
            }
        }
        return null;
    }
    
    public Object compileCreateVisualizationStmt(final CreateVisualizationStmt stmt) throws MetaDataRepositoryException {
        final int count = stmt.fileName.split(".").length - 1;
        if (count > 1) {
            this.error("File name " + stmt.fileName + " is problem", stmt.name);
        }
        final File f = new File(stmt.fileName);
        if (!f.exists()) {
            this.error("File name " + stmt.fileName + " does not exist", stmt.fileName);
        }
        final MetaInfo.Visualization viz = this.ctx.putVisualization(stmt.name, stmt.fileName);
        this.addCurrentApplicationFlowDependencies(viz);
        return null;
    }
    
    public Object UpdateUserInfoStmt(final UpdateUserInfoStmt stmt) {
        try {
            final Map<String, Object> props_toupdate = this.combineProperties(stmt.properties);
            this.ctx.UpdateUserInfoStmt(stmt.username, props_toupdate);
        }
        catch (SecurityException | UnsupportedEncodingException | GeneralSecurityException ex2) {
            this.error("Cannot update user information", stmt.username);
        }
        catch (Exception e) {
            this.error(e.getMessage(), stmt.username);
        }
        return null;
    }
    
    private List<String> generatePermissions(final SecurityStmt stmt) {
        final String name = stmt.getName();
        final List<ObjectPermission.Action> listOfPrivilege = stmt.getListOfPrivilege();
        final List<ObjectPermission.ObjectType> objectType = stmt.getObjectType();
        final ObjectName fullObjectName = this.makeObjectName(name);
        final String objectName = fullObjectName.getName();
        final String objectNamespace = fullObjectName.getNamespace();
        final List<String> permissions = new ArrayList<String>();
        for (int i = 0; i < listOfPrivilege.size(); ++i) {
            final ObjectPermission.Action action = listOfPrivilege.get(i);
            if (objectType != null && !objectType.isEmpty()) {
                for (int j = 0; j < objectType.size(); ++j) {
                    final ObjectPermission.ObjectType objectTypeInstance = objectType.get(j);
                    permissions.add(objectNamespace + Compiler.COLON + ((action == ObjectPermission.Action.all) ? "*" : action) + Compiler.COLON + objectTypeInstance + Compiler.COLON + objectName);
                }
            }
            else {
                permissions.add(objectNamespace + Compiler.COLON + ((action == ObjectPermission.Action.all) ? "*" : action) + Compiler.COLON + "*" + Compiler.COLON + objectName);
            }
        }
        return permissions;
    }
    
    public Object GrantPermissionToStmt(final GrantPermissionToStmt stmt) {
        final List<String> permissions = this.generatePermissions(stmt);
        if (permissions != null) {
            for (final String permission : permissions) {
                if (permission.split(":").length != 4) {
                    this.error("Permission has wrong format :", permission);
                }
                final Pattern pattern = Pattern.compile("\\s");
                final Matcher matcher = pattern.matcher(permission);
                final boolean found = matcher.find();
                if (found) {
                    this.error("Space in the ROLE NAME.", permission);
                }
            }
        }
        if (stmt.towhat == EntityType.ROLE && (stmt.name == null || stmt.name.split(":").length != 2)) {
            this.error("Role name format is wrong. Ex. NameSpaceName:roleName", stmt.name);
        }
        this.ctx.GrantPermissionToStmt(stmt, permissions);
        return null;
    }
    
    public Object GrantRoleToStmt(final GrantRoleToStmt stmt) {
        final Pattern pattern = Pattern.compile("\\s");
        for (final String rolename : stmt.rolename) {
            final Matcher matcher = pattern.matcher(rolename);
            if (rolename.split(":").length != 2) {
                this.error("Rolename has wrong format ", stmt.rolename);
            }
            final boolean found = matcher.find();
            if (found) {
                this.error("Space in the ROLE NAME.", stmt.rolename);
            }
        }
        this.ctx.GrantRoleToStmt(stmt);
        return null;
    }
    
    public Object RevokePermissionFromStmt(final RevokePermissionFromStmt stmt) {
        final List<String> permissions = this.generatePermissions(stmt);
        if (permissions != null) {
            for (final String permission : permissions) {
                if (permission.split(":").length < 4 || permission.split(":").length > 5) {
                    this.error("Permission has wrong format :", permission);
                }
                final Pattern pattern = Pattern.compile("\\s");
                final Matcher matcher = pattern.matcher(permission);
                final boolean found = matcher.find();
                if (found) {
                    this.error("Space in the ROLE NAME.", permission);
                }
            }
        }
        if (stmt.towhat == EntityType.ROLE && (stmt.name == null || stmt.name.split(":").length != 2)) {
            this.error("Role name format is wrong. Ex. NameSpaceName:roleName", stmt.name);
        }
        this.ctx.RevokePermissionFromStmt(stmt, permissions);
        return null;
    }
    
    public Object RevokeRoleFromStmt(final RevokeRoleFromStmt stmt) {
        final Pattern pattern = Pattern.compile("\\s");
        for (final String rolename : stmt.rolename) {
            final Matcher matcher = pattern.matcher(rolename);
            if (rolename.split(":").length != 2) {
                this.error("Rolename has wrong format ", stmt.rolename);
            }
            final boolean found = matcher.find();
            if (found) {
                this.error("Space in the ROLE NAME.", stmt.rolename);
            }
        }
        this.ctx.RevokeRoleFromStmt(stmt);
        return null;
    }
    
    public Object compileLoadOrUnloadJar(final LoadUnloadJarStmt stmt) {
        final HDLoader loader = HDLoader.get();
        final String ns = this.ctx.getCurNamespace().name;
        final int i = stmt.pathToJar.lastIndexOf(47);
        String name;
        if (i == -1) {
            name = stmt.pathToJar;
        }
        else {
            name = stmt.pathToJar.substring(i + 1);
        }
        if (stmt.doLoad) {
            try {
                loader.addJar(ns, stmt.pathToJar, name);
            }
            catch (Exception e) {
                this.error(e.getMessage(), stmt.pathToJar);
            }
        }
        else {
            try {
                loader.removeJar(ns, name);
            }
            catch (Exception e) {
                this.error(e.getMessage(), stmt.pathToJar);
            }
        }
        return null;
    }
    
    public Object compileEndStmt(final EndBlockStmt stmt) throws MetaDataRepositoryException {
        final MetaInfo.Flow flow = this.ctx.getFlowInCurSchema(stmt.name, stmt.type);
        if (flow == null) {
            this.error("cannot find " + stmt.type, stmt.name);
        }
        assert flow.type == stmt.type;
        final String errMsg = this.ctx.endBlock(flow);
        if (errMsg != null) {
            this.error(errMsg, stmt.name);
        }
        return null;
    }
    
    private Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> checkDeploymentRule(final DeploymentRule rule, final MetaInfo.Flow parent, final EntityType type) throws MetaDataRepositoryException {
        final MetaInfo.Flow flow = this.ctx.getFlowInCurSchema(rule.flowName, type);
        if (flow == null) {
            this.error("cannot find such " + type, rule.flowName);
        }
        if (parent != null) {
            if (parent.uuid.equals((Object)flow.uuid)) {
                this.error(type + " cannot be part of itself", rule.flowName);
            }
            final Set<UUID> list = parent.getObjects(EntityType.FLOW);
            if (!list.contains(flow.uuid)) {
                this.error(type + " is not part of <" + parent.name + ">", rule.flowName);
            }
        }
        final MetaInfo.DeploymentGroup dg = this.ctx.getDeploymentGroup(rule.deploymentGroup);
        if (dg == null) {
            this.error("cannot find such deployment group", rule.deploymentGroup);
        }
        return Pair.make(flow, dg);
    }
    
    public Object compileDeployStmt(final DeployStmt stmt) throws MetaDataRepositoryException {
        if (stmt.appOrFlow.isAccessible()) {
            final MetaInfo.DeploymentGroup dg = this.ctx.getDeploymentGroup(stmt.appRule.deploymentGroup);
            if (dg == null) {
                this.error("cannot find such deployment group", stmt.appRule.deploymentGroup);
            }
            final MetaInfo.MetaObject metaObject = this.ctx.get(stmt.appRule.flowName, stmt.appOrFlow);
            this.ctx.remoteCallOnObject(ActionType.LOAD, metaObject, stmt.appRule);
            return null;
        }
        final List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList<MetaInfo.Flow.Detail>();
        this.ctx.resetAppScope();
        final Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> appRule = this.checkDeploymentRule(stmt.appRule, null, stmt.appOrFlow);
        final MetaInfo.Flow app = appRule.first;
        if (!PermissionUtility.checkPermission(app, ObjectPermission.Action.deploy, this.getContext().getSessionID(), false)) {
            this.error("Insufficient permissions to deploy " + app.getName(), app.getName());
        }
        if (!app.getMetaInfoStatus().isValid()) {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsBelongToApp : app.objects.entrySet()) {
                System.out.print(metaObjectsBelongToApp.getKey() + " => [ ");
                for (final UUID metaObjectUUID : metaObjectsBelongToApp.getValue()) {
                    final MetaInfo.MetaObject metaObjectDefinition = this.ctx.getObject(metaObjectUUID);
                    if (metaObjectDefinition == null) {
                        continue;
                    }
                    if (metaObjectDefinition.getMetaInfoStatus().isValid()) {
                        continue;
                    }
                    System.out.print(metaObjectDefinition.name + " ");
                }
                System.out.println("]");
            }
            this.error(stmt.appOrFlow + " is missing components! (Invalid " + stmt.appOrFlow + ")", stmt.appRule.flowName);
        }
        MetaInfo.Flow.Detail.FailOverRule failOverRule = MetaInfo.Flow.Detail.FailOverRule.NONE;
        if (stmt.options != null) {
            for (final Pair<String, String> opt : stmt.options) {
                if (opt.first.equalsIgnoreCase("FAILOVER")) {
                    if (opt.second.equalsIgnoreCase("AUTO")) {
                        failOverRule = MetaInfo.Flow.Detail.FailOverRule.AUTO;
                    }
                    else {
                        if (!opt.second.equalsIgnoreCase("MANUAL")) {
                            continue;
                        }
                        failOverRule = MetaInfo.Flow.Detail.FailOverRule.MANUAL;
                    }
                }
                else {
                    this.error(opt.first + " is not correct option to set for deploy.", stmt.options);
                }
            }
        }
        final MetaInfo.Flow.Detail detail = new MetaInfo.Flow.Detail();
        detail.construct(stmt.appRule.strategy, appRule.second.uuid, appRule.first.uuid, failOverRule);
        deploymentPlan.add(detail);
        for (final DeploymentRule r : stmt.flowRules) {
            final Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> flowRule = this.checkDeploymentRule(r, appRule.first, EntityType.FLOW);
            final MetaInfo.Flow.Detail d = new MetaInfo.Flow.Detail();
            d.construct(r.strategy, flowRule.second.uuid, flowRule.first.uuid, failOverRule);
            deploymentPlan.add(d);
        }
        app.setDeploymentPlan(deploymentPlan);
        try {
            final Map<String, Object> params = (Map<String, Object>)new HashedMap();
            params.put("DEPLOYMENTPLAN", deploymentPlan);
            this.ctx.changeApplicationState(ActionType.DEPLOY, app, params);
            MetaInfo.StatusInfo.Status status = this.getStatus(app.uuid, this.ctx.getAuthToken());
            if (status == MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS) {
                System.out.println("Not enough server(s) in the deployment group to complete the deployment. Waiting for minimum number of the server(s) to join the cluster.....");
            }
            for (int count = 0; status == MetaInfo.StatusInfo.Status.DEPLOYING && count < 300; ++count, status = this.getStatus(app.uuid, this.ctx.getAuthToken())) {
                Thread.sleep(1000L);
            }
        }
        catch (InterruptedException ex) {}
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    
    private MetaInfo.StatusInfo.Status getStatus(final UUID uuid, final AuthToken authToken) throws Exception {
        return FlowUtil.getCurrentStatusFromAppManager(uuid, authToken);
    }
    
    public void compileDumpStmt(final String cqname, final int mode, final int limit, final AuthToken clToken) throws MetaDataRepositoryException {
        this.ctx.changeCQInputOutput(cqname, mode, limit, false, clToken);
    }
    
    public void compileReportStmt(final String flowName, final int action, final List<String> param_list, final AuthToken clToken) throws MetaDataRepositoryException {
        String fileName = null;
        try {
            if (param_list != null && action != 3) {
                for (int i = 0; i < param_list.size(); ++i) {
                    if (!param_list.get(i).equalsIgnoreCase("-export")) {
                        throw new Exception("Unknown parameter - " + param_list.get(i));
                    }
                    if (i < param_list.size() - 1) {
                        fileName = param_list.get(++i);
                    }
                }
            }
            MetaInfo.Flow flowObj = null;
            try {
                final String[] sNames = this.ctx.splitNamespaceAndName(flowName, EntityType.APPLICATION);
                flowObj = (MetaInfo.Flow)this.metadata_repo.getMetaObjectByName(EntityType.APPLICATION, sNames[0], sNames[1], -1, clToken);
                if (flowObj == null) {
                    throw new MetaDataRepositoryException("Unable to find object or no permissions for " + flowName);
                }
            }
            catch (Exception e) {
                this.error(e.getLocalizedMessage(), flowName);
            }
            if (action == 3) {
                CluiMonitorView.showLag(flowObj.getUuid(), param_list, this.ctx.getSessionID());
                return;
            }
            this.ctx.executeReportAction(flowObj.getUuid(), action, fileName, false, clToken);
        }
        catch (Exception e2) {
            this.error(e2.getLocalizedMessage(), flowName);
        }
    }
    
    private void printStatus(final MetaInfo.Flow app, final Collection<Context.Result> r) {
        MetaInfo.StatusInfo.Status stat = null;
        try {
            final ChangeApplicationStateResponse result = this.ctx.changeApplicationState(ActionType.STATUS, app, null);
            stat = ((ApplicationStatusResponse)result).getStatus();
            System.out.println(app.name + " is " + stat);
            if (stat == MetaInfo.StatusInfo.Status.CRASH) {
                final Set<ExceptionEvent> exceptionEvents = ((ApplicationStatusResponse)result).getExceptionEvents();
                System.out.println("Exception(s) leading to CRASH State:");
                for (final ExceptionEvent event : exceptionEvents) {
                    System.out.println("    " + event.userString());
                }
            }
            if (stat == MetaInfo.StatusInfo.Status.DEPLOYED || stat == MetaInfo.StatusInfo.Status.RUNNING || stat == MetaInfo.StatusInfo.Status.CRASH) {
                System.out.println("Status per node....");
                System.out.println(r);
            }
        }
        catch (Exception e) {
            Compiler.logger.error((Object)("Failed to get status with exception " + e.getMessage()), (Throwable)e);
        }
    }
    
    public Object compileActionStmt(final ActionStmt stmt) throws MetaDataRepositoryException {
        if (stmt.type.isAccessible()) {
            final MetaInfo.MetaObject metaObject = this.ctx.get(stmt.name, stmt.type);
            if (metaObject instanceof MetaObjectPermissionChecker && !((MetaObjectPermissionChecker)metaObject).checkPermissionForMetaPropertyVariable(this.getContext().getSessionID())) {
                Compiler.logger.error((Object)"Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
            }
            this.ctx.remoteCallOnObject(ActionType.UNLOAD, metaObject, new Object[0]);
            return null;
        }
        final MetaInfo.Flow app = this.ctx.getFlowInCurSchema(stmt.name, stmt.type);
        if (app == null) {
            throw new Warning("cannot find " + stmt.type + " " + stmt.name + " in current namespace <" + this.ctx.getCurNamespace().name + ">");
        }
        final Set<UUID> allObjectsInApp = app.getDeepDependencies();
        for (final UUID uuid : allObjectsInApp) {
            final MetaInfo.MetaObject obj = this.metadata_repo.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
            if (obj instanceof MetaObjectPermissionChecker && !((MetaObjectPermissionChecker)obj).checkPermissionForMetaPropertyVariable(this.getContext().getSessionID())) {
                Compiler.logger.error((Object)"Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
            }
        }
        ObjectPermission.Action action = null;
        switch (stmt.what) {
            case START: {
                action = ObjectPermission.Action.start;
                break;
            }
            case STOP: {
                action = ObjectPermission.Action.stop;
                break;
            }
            case QUIESCE: {
                action = ObjectPermission.Action.quiesce;
                break;
            }
            case UNDEPLOY: {
                action = ObjectPermission.Action.undeploy;
                break;
            }
            case STATUS: {
                action = ObjectPermission.Action.read;
                break;
            }
            case MEMORY_STATUS: {
                action = ObjectPermission.Action.memory;
                break;
            }
            case RESUME: {
                action = ObjectPermission.Action.resume;
                break;
            }
            default: {
                this.error(stmt.what + " is not an allowable action for ", stmt.name);
                break;
            }
        }
        if (!PermissionUtility.checkPermission(app, action, this.getContext().getSessionID(), false)) {
            this.error("Insufficient permissions to " + action + " " + stmt.name, stmt.name);
        }
        assert app.type == stmt.type;
        Collection<Context.Result> r = null;
        if (stmt.what == ActionType.START || stmt.what == ActionType.STOP || stmt.what == ActionType.UNDEPLOY || stmt.what == ActionType.RESUME || stmt.what == ActionType.QUIESCE) {
            final Map<String, Object> params = (Map<String, Object>)new HashedMap();
            params.put("RECOVERYDESCRIPTION", stmt.recov);
            try {
                this.ctx.changeApplicationState(stmt.what, app, params);
            }
            catch (Exception e) {
                throw new Warning(e.getMessage(), e);
            }
        }
        else {
            r = this.ctx.changeFlowState(stmt.what, app);
        }
        if (stmt.what == ActionType.STATUS) {
            this.printStatus(app, r);
        }
        return null;
    }
    
    private Pair<MetaInfo.Sorter.SorterRule, Class<?>> makeSorterRule(final SorterInOutRule inRule) throws MetaDataRepositoryException {
        final MetaInfo.Stream inStream = this.ctx.getStreamInCurSchema(inRule.inStream);
        if (inStream == null) {
            this.error("no such stream", inRule.inStream);
        }
        final List<Field> flds = this.checkPartitionFields(Collections.singletonList(inRule.inStreamField), inStream.dataType, inRule.inStream);
        final String outName = inRule.outStream;
        MetaInfo.Stream outStream = this.ctx.getStreamInCurSchema(outName);
        if (outStream == null) {
            this.checkCompoundStreamName(outName);
            outStream = this.ctx.putStream(false, this.makeObjectName(outName), inStream.dataType, null, null, null, null, null);
        }
        else if (!outStream.dataType.equals((Object)inStream.dataType)) {
            this.error("input and output streams should have same data type", outName);
        }
        final MetaInfo.Sorter.SorterRule rule = new MetaInfo.Sorter.SorterRule(inStream.uuid, outStream.uuid, inRule.inStreamField, inStream.dataType);
        final Class<?> keyFldType = flds.get(0).getType();
        return Pair.make(rule, keyFldType);
    }
    
    public Object compileCreateSorterStmt(final CreateSorterStmt stmt) throws MetaDataRepositoryException {
        final List<MetaInfo.Sorter.SorterRule> inOutRules = new ArrayList<MetaInfo.Sorter.SorterRule>();
        Class<?> keyFldType = null;
        for (final SorterInOutRule sr : stmt.inOutRules) {
            final Pair<MetaInfo.Sorter.SorterRule, Class<?>> r = this.makeSorterRule(sr);
            inOutRules.add(r.first);
            if (keyFldType == null) {
                keyFldType = r.second;
            }
            else {
                if (keyFldType == r.second) {
                    continue;
                }
                final String firstFld = stmt.inOutRules.get(0).inStreamField;
                final String firstFldType = keyFldType.getCanonicalName();
                final String thisFldType = r.second.getCanonicalName();
                this.error("key field has type [" + thisFldType + "] incompatible with type [" + firstFldType + "] of first key field <" + firstFld + ">", sr.inStreamField);
            }
        }
        MetaInfo.Stream errorStream = this.ctx.getStreamInCurSchema(stmt.errorStream);
        if (errorStream == null) {
            this.checkCompoundStreamName(stmt.errorStream);
            errorStream = this.ctx.putStream(false, this.makeObjectName(stmt.errorStream), UUID.nilUUID(), null, null, null, null, null);
        }
        this.ctx.putSorter(stmt.doReplace, this.makeObjectName(stmt.name), stmt.sortTimeInterval, inOutRules, errorStream.uuid);
        return null;
    }
    
    public Object compileCreateDashboardStatement(final CreateDashboardStatement stmt) throws MetaDataRepositoryException {
        final String json = readFile(stmt);
        try {
            this.ctx.createDashboard(json, null);
        }
        catch (Exception e) {
            throw e;
        }
        return null;
    }
    
    public static String readFile(final CreateDashboardStatement stmt) {
        final File f = new File(stmt.getFileName());
        if (!f.exists()) {
            throw new RuntimeException("File name " + stmt.getFileName() + " does not exist");
        }
        Scanner scanner = null;
        try {
            scanner = new Scanner(f);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String json = "";
        if (scanner != null) {
            while (scanner.hasNextLine()) {
                json += scanner.nextLine();
            }
            scanner.close();
            return json;
        }
        throw new RuntimeException("File creator is failing.");
    }
    
    public Object execPreparedQuery(final ExecPreparedStmt stmt) throws MetaDataRepositoryException {
        final MetaInfo.Query q = this.ctx.get(stmt.queryName, EntityType.QUERY);
        if (q == null) {
            this.error("no such name query", stmt.queryName);
        }
        if (!q.getMetaInfoStatus().isValid()) {
            throw new FatalException("Query is not valid, either metaobject is changed/missing. \nTo make query executable, please recompile query by: \nW (...) > alter namedquery " + stmt.queryName + " recompile.");
        }
        final MetaInfo.Query dup = this.ctx.prepareQuery(q, stmt.params);
        return dup;
    }
    
    void validateNestedTQL(final File filePointer) throws FileNotFoundException {
        byte[] encoded;
        try {
            encoded = Files.readAllBytes(Paths.get(filePointer.toURI()));
        }
        catch (IOException e) {
            throw new FileNotFoundException(e.getMessage());
        }
        final String tqlText = Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
        final Lexer lexer = new Lexer(tqlText, false);
        final Grammar parser = new Grammar(lexer, lexer.getSymbolFactory());
        try {
            final List<Stmt> stmts = parser.parseStmt(false);
            if (stmts != null) {
                for (final Stmt stmt : stmts) {
                    if (stmt instanceof LoadFileStmt) {
                        final File file = new File(((LoadFileStmt)stmt).getFileName());
                        this.validateNestedTQL(file);
                    }
                }
            }
        }
        catch (Exception e2) {
            if (e2 instanceof FileNotFoundException) {
                throw (FileNotFoundException)e2;
            }
            throw new CompilationException(e2.getMessage());
        }
    }
    
    public Object compileLoadFileStmt(final LoadFileStmt loadFileStmt) throws Exception {
        final File filePointer = new File(loadFileStmt.getFileName());
        final byte[] encoded = Files.readAllBytes(Paths.get(filePointer.toURI()));
        final String text = Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
        this.validateNestedTQL(filePointer);
        if (HazelcastSingleton.isClientMember()) {
            Tungsten.processWithContext(text, this.ctx);
        }
        else {
            compile(text, this.ctx, new ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    try {
                        compiler.compileStmt(stmt);
                    }
                    catch (Warning e) {
                        Compiler.logger.warn((Object)e.getMessage());
                    }
                }
            });
        }
        return null;
    }
    
    public void compileMonitorStmt(final MonitorStmt monitorStmt) throws MetaDataRepositoryException {
        CluiMonitorView.handleMonitorRequest(monitorStmt.params);
    }
    
    public Object compileExportTypesStmt(final ExportAppStmt exportAppStmt) throws MetaDataRepositoryException {
        final String namespace = this.ctx.getNamespace(Utility.splitDomain(exportAppStmt.appName)).getName();
        final String appName = Utility.splitName(exportAppStmt.appName);
        final File targetFile = new File(exportAppStmt.filepath);
        try {
            final List<MetaInfo.MetaObject> moList = MetadataRepository.getINSTANCE().getAllObjectsInApp(namespace, appName, false, HSecurityManager.TOKEN);
            if (moList.size() == 0) {
                throw new Exception("No application found with the name '" + namespace + "." + appName + "'");
            }
            if (targetFile.isDirectory()) {
                throw new Exception("Specified path is a directory.Specify path of the jar file to be generated.");
            }
            if (targetFile.exists()) {
                targetFile.delete();
            }
            final FileOutputStream fos = new FileOutputStream(targetFile);
            if (exportAppStmt.format.equals("JAR")) {
                this.generateTypesAsJar(moList, fos, namespace);
            }
            fos.close();
            System.out.println("Exported application jar to " + targetFile.getAbsolutePath() + " ");
        }
        catch (Exception e) {
            if (targetFile.exists()) {
                targetFile.delete();
            }
            throw new MetaDataRepositoryException("Failed to export types : " + e.getMessage());
        }
        return null;
    }
    
    public void generateTypesAsJar(final List<MetaInfo.MetaObject> moList, final FileOutputStream fos, final String namespace) throws Exception {
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        final JarOutputStream target = new JarOutputStream(fos, manifest);
        addJarEntryForPackage("wa." + namespace + ".", target);
        for (final MetaInfo.MetaObject mo : moList) {
            if (mo.getType() == EntityType.TYPE) {
                final String tn = "wa." + mo.getNsName() + "." + mo.getName() + "_1_0";
                addJarEntryForTypeClass(tn, target);
                System.out.println(mo.getNsName() + "." + mo.getName() + " exported successfully ");
            }
        }
        target.close();
    }
    
    static void addJarEntryForTypeClass(final String typeName, final JarOutputStream target) throws IOException {
        final String fqn = typeName.replace('.', '/');
        final JarEntry jarEntry = new JarEntry(fqn + ".class");
        jarEntry.setTime(System.currentTimeMillis());
        target.putNextEntry(jarEntry);
        final byte[] typeClassByteCode = HDLoader.get().getClassBytes(typeName);
        target.write(typeClassByteCode, 0, typeClassByteCode.length);
        target.closeEntry();
    }
    
    static void addJarEntryForPackage(final String typeName, final JarOutputStream target) throws IOException {
        String packageName = typeName.replace('.', '/');
        if (!packageName.endsWith("/")) {
            packageName += "/";
        }
        final JarEntry jarEntry = new JarEntry(packageName);
        jarEntry.setTime(System.currentTimeMillis());
        target.putNextEntry(jarEntry);
        target.closeEntry();
    }
    
    public ObjectName makeObjectName(final String name) {
        return this.ctx.makeObjectName(name);
    }
    
    public ObjectName makeObjectName(final String namespace, final String name) {
        return this.ctx.makeObjectName(namespace, name);
    }
    
    public MetaInfo.Type prepareSource(final String adapterTypeName, final AdapterDescription sourceProperties, final AdapterDescription parserProperties) throws MetaDataRepositoryException {
        final List<Property> handler_props = sourceProperties.getProps();
        final String adapterVersion = sourceProperties.getVersion();
        final String parserOrFormatterHandler = (parserProperties != null) ? parserProperties.getAdapterTypeName() : null;
        final List<Property> parserOrFormatterProps = (parserProperties != null) ? parserProperties.getProps() : Collections.emptyList();
        final String parserVersion = (parserProperties != null && parserProperties.getVersion() != null) ? parserProperties.getVersion() : null;
        final MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(adapterTypeName, adapterVersion);
        if (pt == null) {
            this.error("cannot load adapter", adapterTypeName);
        }
        if ((pt.requiresParser || pt.requiresFormatter) && parserOrFormatterHandler == null) {
            this.error(sourceProperties.getAdapterTypeName() + " requires parser.", sourceProperties.getAdapterTypeName());
        }
        if (pt.adapterType != AdapterType.source && pt.adapterType != AdapterType.internal) {
            this.error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for sources.", adapterTypeName);
        }
        MetaInfo.PropertyTemplateInfo parserOrFormatterPropertyTemplate = null;
        if (parserOrFormatterHandler != null) {
            parserOrFormatterPropertyTemplate = this.ctx.getAdapterProperties(parserOrFormatterHandler, parserVersion);
            if (parserOrFormatterPropertyTemplate == null) {
                this.error("cannot find parser template", parserOrFormatterHandler);
            }
            if (parserOrFormatterPropertyTemplate.adapterType != AdapterType.parser && parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal) {
                this.error("The specified parser is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct parser adapter for sources.", adapterTypeName);
            }
        }
        final Map<String, Object> props = this.combineProperties(handler_props);
        final Map<String, Object> parserOrFormatterProperties = this.combineProperties(parserOrFormatterProps);
        if (parserOrFormatterHandler != null) {
            parserOrFormatterProperties.put("handler", parserOrFormatterPropertyTemplate.className);
        }
        this.validateAdapterPropsAndValues(pt, props, handler_props);
        if (parserOrFormatterPropertyTemplate != null) {
            this.validateAdapterPropsAndValues(parserOrFormatterPropertyTemplate, parserOrFormatterProperties, null);
        }
        final Class<?> inputTypeClass = (parserOrFormatterPropertyTemplate == null) ? pt.getInputClass() : parserOrFormatterPropertyTemplate.getInputClass();
        final MetaInfo.Type inputType = this.getTypeForCLass(inputTypeClass);
        return inputType;
    }
    
    public Object createFilteredSource(final CreateSourceOrTargetStmt createSourceWithImplicitCQStmt) throws MetaDataRepositoryException {
        return new SourceSideFilteringManager().receive(new SourceSideFilteringHandler(), this, createSourceWithImplicitCQStmt);
    }
    
    public Object compileAlterStmt(final AlterStmt alterStmt) {
        try {
            final MetaInfo.Stream streamMO = this.ctx.get(this.makeObjectName(alterStmt.getObjectName()).getFullName(), EntityType.STREAM);
            if (alterStmt.getPartitionBy() != null && !alterStmt.getPartitionBy().isEmpty()) {
                this.checkPartitionFields(alterStmt.getPartitionBy(), streamMO.getDataType(), streamMO.getFullName());
                if (alterStmt.getEnablePartitionBy()) {
                    if (alterStmt.getPartitionBy() == null) {
                        throw new CompilationException("Alter operation failed, please retry again");
                    }
                    final Set partitionBySet = new LinkedHashSet(streamMO.partitioningFields);
                    partitionBySet.addAll(alterStmt.getPartitionBy());
                    streamMO.partitioningFields = new ArrayList<String>(partitionBySet);
                }
                else {
                    streamMO.partitioningFields.removeAll(alterStmt.getPartitionBy());
                }
            }
            if (alterStmt.getEnablePersistence() != null) {
                if (alterStmt.getEnablePersistence()) {
                    final String namespace = Utility.splitDomain(alterStmt.getPersistencePolicy().getFullyQualifiedNameOfPropertyset());
                    final String name = Utility.splitName(alterStmt.getPersistencePolicy().getFullyQualifiedNameOfPropertyset());
                    final MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)this.metadata_repo.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, HSecurityManager.TOKEN);
                    if (kpset == null) {
                        return null;
                    }
                    streamMO.setPset(kpset.getFullName());
                    this.setupKafka(streamMO, false);
                }
                else {
                    try {
                        this.destroyTopic(streamMO);
                    }
                    catch (Exception ex) {}
                    streamMO.setPset(null);
                    streamMO.setPropertySet(null);
                }
            }
            final String[] partitionFields = streamMO.partitioningFields.toArray(new String[0]);
            final String persistPropertySetName = streamMO.pset;
            final MetaInfo.Type streamType = (MetaInfo.Type)this.metadata_repo.getMetaObjectByUUID(streamMO.dataType, HSecurityManager.TOKEN);
            streamMO.setSourceText(Utility.createStreamStatementText(streamMO.getName(), false, streamType.name, partitionFields, persistPropertySetName));
            this.ctx.updateMetaObject(streamMO);
        }
        catch (Exception e) {
            throw new AlterException(e.getMessage(), e);
        }
        return null;
    }
    
    private void destroyTopic(final MetaInfo.Stream streamMO) {
        try {
            if (HazelcastSingleton.isClientMember()) {
                final RemoteCall deleteTopic_executor = KafkaStreamUtils.getDeleteTopicExecutor(streamMO);
                DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Object>)deleteTopic_executor);
            }
            else {
                KafkaStreamUtils.deleteTopic(streamMO);
            }
        }
        catch (Exception e) {
            Compiler.logger.warn((Object)("Failed to delete kafka topics associated with stream: " + streamMO.getFullName() + ", Reason: " + e.getMessage() + ".Please delete topics (" + KafkaStreamUtils.createTopicName(streamMO) + ", " + KafkaStreamUtils.getCheckpointTopicName(KafkaStreamUtils.createTopicName(streamMO)) + " manually!"), (Throwable)e);
        }
    }
    
    static {
        Compiler.logger = Logger.getLogger((Class)Compiler.class);
        NAMED_QUERY_PREFIX = "namedQuery" + java.util.UUID.nameUUIDFromBytes("namedQuery".getBytes()).toString().split("-")[0];
        Compiler.COLON = ":";
    }
    
    public static class FileMetadataExtensionResult
    {
        private Set<FileMetadataExtension> result;
        private String msg;
        private String appName;
        
        public FileMetadataExtensionResult(final Set<FileMetadataExtension> result, final String msg, final String appName) {
            this.result = result;
            this.msg = msg;
            this.appName = appName;
        }
        
        public Set<FileMetadataExtension> getResult() {
            return this.result;
        }
        
        public String getMsg() {
            return this.msg;
        }
        
        public String getAppName() {
            return this.appName;
        }
    }
    
    public interface ExecutionCallback
    {
        void execute(final Stmt p0, final Compiler p1) throws Exception;
    }
}
