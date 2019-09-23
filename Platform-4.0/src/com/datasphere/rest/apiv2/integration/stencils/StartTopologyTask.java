package com.datasphere.rest.apiv2.integration.stencils;

import com.datasphere.uuid.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.components.*;
import java.util.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.metaRepository.*;
import java.sql.*;

public class StartTopologyTask implements Runnable
{
    private AuthToken token;
    private String topologyName;
    
    public StartTopologyTask(final AuthToken token, final String topologyName) {
        this.token = token;
        this.topologyName = topologyName;
    }
    
    @Override
    public void run() {
        final String initialLoadAppFqn = this.topologyName + "." + "InitialLoadApp";
        final String cdcAppFqn = this.topologyName + "." + "IncrementalDataApp";
        try {
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(this.token);
            queryValidator.setUpdateMode(true);
            this.deployApp(queryValidator, initialLoadAppFqn, "InitialLoadSourceFlow", "InitialLoadTargetFlow");
            final String scn = this.getCurrentScnOfSourceDB();
            this.startApp(queryValidator, initialLoadAppFqn);
            this.alterRecompileCDCApp(queryValidator, cdcAppFqn, scn);
            this.deployApp(queryValidator, cdcAppFqn, "IncrementalDataSourceFlow", "IncrementalDataTargetFlow");
            this.monitorILapp();
            this.startApp(queryValidator, cdcAppFqn);
        }
        catch (InterruptedException ie) {}
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void deployApp(final QueryValidator queryValidator, final String appFqn, final String sourceFlow, final String targetFlow) throws MetaDataRepositoryException {
        final Map<String, String> appDescription = new HashMap<String, String>() {
            {
                this.put("flow", appFqn);
                this.put("strategy", "any");
            }
        };
        final Map<String, String>[] detailDescriptions = null;
        queryValidator.setCurrentApp(appFqn, this.token);
        queryValidator.CreateDeployFlowStatement(this.token, EntityType.APPLICATION.name(), appDescription, detailDescriptions);
    }
    
    private String getCurrentScnOfSourceDB() throws MetaDataRepositoryException, SQLException {
        final MetaInfo.PropertySet propset = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, this.topologyName, "StencilProperties", null, this.token);
        final String jdbcUrl = (String)propset.getProperties().get("SourceDatabaseJdbcUrl");
        final String username = (String)propset.getProperties().get("SourceDatabaseUsername");
        final String password = (String)propset.getProperties().get("SourceDatabasePassword");
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery("select current_scn from V$database");
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        return null;
    }
    
    private void startApp(final QueryValidator queryValidator, final String appFqn) throws MetaDataRepositoryException {
        queryValidator.setCurrentApp(appFqn, this.token);
        queryValidator.CreateStartFlowStatement(this.token, appFqn, EntityType.APPLICATION.name());
    }
    
    private void alterRecompileCDCApp(final QueryValidator queryValidator, final String cdcAppFqn, final String scn) throws MetaDataRepositoryException {
        final MetaInfo.Source source = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.SOURCE, this.topologyName, "IncrementalDataSource", null, this.token);
        if (source != null) {
            source.getProperties().put("StartSCN", scn);
            MetadataRepository.getINSTANCE().putMetaObject(source, this.token);
        }
    }
    
    private void monitorILapp() throws MetaDataRepositoryException, InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, this.topologyName, "InitialLoadApp", null, this.token);
            if (application != null && application.getFlowStatus() == MetaInfo.StatusInfo.Status.DEPLOYED) {
                break;
            }
            Thread.sleep(1000L);
        }
    }
}
