package com.datasphere.rest.apiv2.integration.stencils;

import com.datasphere.kafkamessaging.*;
import com.datasphere.concurrency.*;
import java.util.concurrent.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.compiler.stmts.*;

import java.util.*;
import com.datasphere.security.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.components.*;
import com.datasphere.rest.apiv2.integration.models.*;
import com.datasphere.runtime.*;

public class OracleToSqldbStencil extends BaseStencil
{
    public static final String STENCIL_NAME = "oracle-to-sqldb";
    public static final String INITIAL_LOAD_APPNAME = "InitialLoadApp";
    public static final String INITIAL_LOAD_STREAMNAME = "InitialLoadStream";
    public static final String INITIAL_LOAD_SOURCENAME = "InitialLoadSource";
    public static final String INITIAL_LOAD_SOURCEFLOWNAME = "InitialLoadSourceFlow";
    public static final String INITIAL_LOAD_TARGETNAME = "InitialLoadTarget";
    public static final String INITIAL_LOAD_TARGETFLOWNAME = "InitialLoadTargetFlow";
    public static final String CDC_APPNAME = "IncrementalDataApp";
    public static final String CDC_STREAMNAME = "IncrementalDataStream";
    public static final String CDC_SOURCENAME = "IncrementalDataSource";
    public static final String CDC_SOURCEFLOWNAME = "IncrementalDataSourceFlow";
    public static final String CDC_TARGETNAME = "IncrementalDataTarget";
    public static final String CDC_TARGETFLOWNAME = "IncrementalDataTargetFlow";
    public static final String SOURCEDB_JDBC_URL_KEY = "SourceDatabaseJdbcUrl";
    public static final String SOURCEDB_USERNAME_KEY = "SourceDatabaseUsername";
    public static final String SOURCEDB_PASSWORD_KEY = "SourceDatabasePassword";
    private ExecutorService topologyStartService;
    private Map<String, Future> topologyToTaskMap;
    
    public OracleToSqldbStencil() {
        this.topologyStartService = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), (ThreadFactory)new NamedThreadFactory("IntegrationApiService"));
        this.topologyToTaskMap = new HashMap<String, Future>();
    }
    
    public static void createApplication(final AuthToken token, final QueryValidator qv, final String namespace, final Map<String, String> sourceProperties, final Map<String, String> targetProperties, final String appname, final String streamName, final String sourceName, final String sourceFlowName, final String targetName, final String targetFlowName, final String sourceAdapterName) throws MetaDataRepositoryException {
        qv.CreateAppStatement(token, appname, true, null, false, null, null, null, null);
        qv.setCurrentApp(namespace + "." + appname, token);
        qv.CreateStreamStatement(token, streamName, true, new String[0], "Global.WAEvent", new StreamPersistencePolicy(null));
        qv.CreateFlowStatement(token, sourceFlowName, true, null);
        qv.setCurrentFlow(namespace + "." + sourceFlowName, token);
        final List<Property> sourcePropList = new ArrayList<Property>();
        for (final Map.Entry<String, String> entry : sourceProperties.entrySet()) {
            sourcePropList.add(new Property(entry.getKey(), entry.getValue()));
        }
        qv.CreateSourceStatement_New(token, sourceName, true, sourceAdapterName, null, sourcePropList, null, null, null, streamName);
        qv.setCurrentFlow(null, token);
        qv.CreateFlowStatement(token, targetFlowName, true, null);
        qv.setCurrentFlow(namespace + "." + targetFlowName, token);
        final Map<String, String>[] props = (Map<String, String>[])new Map[] { targetProperties };
        qv.CreateTargetStatement(token, targetName, true, "DatabaseWriter", null, props, null, null, null, streamName);
        qv.setCurrentFlow(null, token);
        qv.setCurrentApp(null, token);
    }
    
    public static void createInitialApplication(final AuthToken token, final QueryValidator qv, final String namespace, final Map<String, String> sourceProperties, final Map<String, String> targetProperties) throws MetaDataRepositoryException {
        createApplication(token, qv, namespace, sourceProperties, targetProperties, "InitialLoadApp", "InitialLoadStream", "InitialLoadSource", "InitialLoadSourceFlow", "InitialLoadTarget", "InitialLoadTargetFlow", "DatabaseReader");
    }
    
    public static void createIncrementalApplication(final AuthToken token, final QueryValidator qv, final String namespace, final Map<String, String> sourceProperties, final Map<String, String> targetProperties) throws MetaDataRepositoryException {
        createApplication(token, qv, namespace, sourceProperties, targetProperties, "IncrementalDataApp", "IncrementalDataStream", "IncrementalDataSource", "IncrementalDataSourceFlow", "IncrementalDataTarget", "IncrementalDataTargetFlow", "OracleReader");
    }
    
    public static StencilParameter.ParameterTypeEnum convertParameterTypeClassToEnum(final Class<?> clazz) {
        if (clazz == Boolean.class) {
            return StencilParameter.ParameterTypeEnum.BOOL;
        }
        if (clazz == Integer.class || clazz == Long.class) {
            return StencilParameter.ParameterTypeEnum.INT;
        }
        if (clazz == Password.class) {
            return StencilParameter.ParameterTypeEnum.SECURESTRING;
        }
        return StencilParameter.ParameterTypeEnum.STRING;
    }
    
    @Override
    public Stencil getStencilModel(final AuthToken token) throws MetaDataRepositoryException {
        final Stencil oracleToSqlDb = new Stencil();
        oracleToSqlDb.setStencilName("oracle-to-sqldb");
        MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", "OracleReader", null, token);
        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
            final StencilParameter parameter = new StencilParameter();
            parameter.setName(entry.getKey());
            parameter.setRequired(entry.getValue().isRequired());
            parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
            oracleToSqlDb.getSourceParameters().add(parameter);
        }
        pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", "DatabaseReader", null, token);
        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
            final StencilParameter parameter = new StencilParameter();
            parameter.setName("IL" + entry.getKey());
            parameter.setRequired(entry.getValue().isRequired());
            parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
            oracleToSqlDb.getSourceParameters().add(parameter);
        }
        pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", "DatabaseWriter", null, token);
        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
            final StencilParameter parameter = new StencilParameter();
            parameter.setName(entry.getKey());
            parameter.setRequired(entry.getValue().isRequired());
            parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
            oracleToSqlDb.getTargetParameters().add(parameter);
        }
        oracleToSqlDb.addLink("self", new ArrayList<String>() {
            {
                this.add("GET");
            }
        }, "/api/v2/integrations/stencils/" + oracleToSqlDb.getStencilName());
        return oracleToSqlDb;
    }
    
    @Override
    public void createTopology(final AuthToken token, final TopologyRequest topologyRequestParameters) throws MetaDataRepositoryException {
        super.createTopology(token, topologyRequestParameters);
        final String namespace = topologyRequestParameters.getTopologyName();
        final QueryValidator queryValidator = new QueryValidator(Server.getServer());
        queryValidator.createNewContext(token);
        queryValidator.setUpdateMode(true);
        queryValidator.CreateUseSchemaStmt(token, namespace);
        final Map<String, String> sourceProperties = topologyRequestParameters.getSourceParameters();
        final Map<String, String> ilSourceProperties = new HashMap<String, String>();
        ilSourceProperties.putAll(sourceProperties);
        ilSourceProperties.put("Username", ilSourceProperties.get("ILUsername"));
        ilSourceProperties.put("Password", ilSourceProperties.get("ILPassword"));
        ilSourceProperties.put("ConnectionURL", ilSourceProperties.get("ILConnectionURL"));
        ilSourceProperties.put("SendMarkerOnEOD", "True");
        ilSourceProperties.remove("ILUsername");
        ilSourceProperties.remove("ILPassword");
        ilSourceProperties.remove("ILConnectionURL");
        sourceProperties.remove("ILUsername");
        sourceProperties.remove("ILPassword");
        sourceProperties.remove("ILConnectionURL");
        createInitialApplication(token, queryValidator, namespace, ilSourceProperties, topologyRequestParameters.getTargetParameters());
        createIncrementalApplication(token, queryValidator, namespace, sourceProperties, topologyRequestParameters.getTargetParameters());
        final Map<String, String>[] props = (Map<String, String>[])new Map[] { new HashMap() };
        props[0].put("StencilName", "oracle-to-sqldb");
        props[0].put("SourceDatabaseJdbcUrl", ilSourceProperties.get("ConnectionURL"));
        props[0].put("SourceDatabaseUsername", sourceProperties.get("Username"));
        props[0].put("SourceDatabasePassword", sourceProperties.get("Password"));
        queryValidator.CreatePropertySet(token, "StencilProperties", true, props);
        queryValidator.removeContext(token);
    }
    
    @Override
    public void startTopology(final AuthToken token, final String topologyName) throws MetaDataRepositoryException {
        if (this.topologyToTaskMap.containsKey(topologyName)) {
            throw new IllegalArgumentException("Topology already started");
        }
        final Future future = this.topologyStartService.submit(new StartTopologyTask(token, topologyName));
        this.topologyToTaskMap.put(topologyName, future);
    }
    
    @Override
    public void stopTopology(final AuthToken token, final String topologyName) throws MetaDataRepositoryException {
        final String initialLoadAppFqn = topologyName + "." + "InitialLoadApp";
        final String cdcAppFqn = topologyName + "." + "IncrementalDataApp";
        final QueryValidator queryValidator = new QueryValidator(Server.getServer());
        queryValidator.createNewContext(token);
        queryValidator.setUpdateMode(true);
        final MetaInfo.Flow cdcAppFlow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, topologyName, "IncrementalDataApp", null, token);
        queryValidator.setCurrentApp(cdcAppFqn, token);
        if (cdcAppFlow.getFlowStatus() == MetaInfo.StatusInfo.Status.RUNNING) {
            queryValidator.CreateStopFlowStatement(token, cdcAppFqn, EntityType.APPLICATION.name());
            queryValidator.CreateUndeployFlowStatement(token, cdcAppFqn, EntityType.APPLICATION.name());
        }
        else if (cdcAppFlow.getFlowStatus() == MetaInfo.StatusInfo.Status.DEPLOYED) {
            queryValidator.CreateUndeployFlowStatement(token, cdcAppFqn, EntityType.APPLICATION.name());
        }
        final MetaInfo.Flow ilAppFlow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, topologyName, "InitialLoadApp", null, token);
        queryValidator.setCurrentApp(initialLoadAppFqn, token);
        if (ilAppFlow.getFlowStatus() == MetaInfo.StatusInfo.Status.RUNNING) {
            queryValidator.CreateStopFlowStatement(token, initialLoadAppFqn, EntityType.APPLICATION.name());
            queryValidator.CreateUndeployFlowStatement(token, initialLoadAppFqn, EntityType.APPLICATION.name());
        }
        else if (ilAppFlow.getFlowStatus() == MetaInfo.StatusInfo.Status.DEPLOYED) {
            queryValidator.CreateUndeployFlowStatement(token, initialLoadAppFqn, EntityType.APPLICATION.name());
        }
    }
    
    @Override
    public void dropTopology(final AuthToken token, final String topologyName) throws MetaDataRepositoryException {
        super.dropTopology(token, topologyName);
    }
}
