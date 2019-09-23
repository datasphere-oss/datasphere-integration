package com.datasphere.runtime.monitor;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import com.datasphere.uuid.*;
import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;
import java.util.*;

public class MonitorApp
{
    private static Logger logger;
    private static String appStmt;
    
    public static void getMonitorApp() throws MetaDataRepositoryException {
        if (!MonitorModel.monitorIsEnabled()) {
            if (MonitorApp.logger.isInfoEnabled()) {
                MonitorApp.logger.info((Object)"HD Monitor is not enabled");
            }
            return;
        }
        final Server srv = Server.server;
        final Context ctx = srv.createContext();
        final MetadataRepository instance = MetadataRepository.getINSTANCE();
        final String name = ctx.getCurNamespace().getName();
        final AuthToken authToken = ctx.getAuthToken();
        final ILock lock = HazelcastSingleton.get().getLock("CreateMonitoringAppLock");
        lock.lock();
        boolean needCreate = false;
        boolean needDeploy = false;
        try {
            MetaInfo.Flow monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, "MonitoringSourceApp", null, authToken);
            if (monitoringApp != null) {
                if (MonitorApp.logger.isInfoEnabled()) {
                    MonitorApp.logger.info((Object)"Monitoring App Exists, Checking Deployment");
                }
                if (monitoringApp.deploymentPlan == null || monitoringApp.deploymentPlan.size() == 0 || !monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING)) {
                    if (MonitorApp.logger.isInfoEnabled()) {
                        MonitorApp.logger.info((Object)"Monitoring App Exists, but Source App not deployed properly");
                    }
                    needDeploy = true;
                }
                monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, "MonitoringProcessApp", null, authToken);
                if (monitoringApp.deploymentPlan == null || monitoringApp.deploymentPlan.size() == 0 || !monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING)) {
                    if (MonitorApp.logger.isInfoEnabled()) {
                        MonitorApp.logger.info((Object)"Monitoring App Exists, but Processing App not deployed properly");
                    }
                    needDeploy = true;
                }
            }
            else {
                needCreate = true;
                needDeploy = true;
            }
            if (needCreate) {
                final QueryValidator qv = new QueryValidator(srv);
                qv.addContext(authToken, ctx);
                qv.setUpdateMode(true);
                try {
                    qv.compileText(authToken, MonitorApp.appStmt);
                }
                catch (Exception e) {
                    MonitorApp.logger.error((Object)"Problem creating Monitoring App", (Throwable)e);
                }
            }
            if (needDeploy) {
                try {
                    deployApplication(ctx, instance, name, authToken, "MonitoringSourceApp", DeploymentStrategy.ON_ALL);
                    deployApplication(ctx, instance, name, authToken, "MonitoringProcessApp", DeploymentStrategy.ON_ONE);
                }
                catch (Exception e2) {
                    MonitorApp.logger.error((Object)"Problem deploying Monitoring App", (Throwable)e2);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public static void deployApplication(final Context ctx, final MetadataRepository instance, final String name, final AuthToken authToken, final String appName, final DeploymentStrategy deploymentStrategy) throws MetaDataRepositoryException {
        final List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList<MetaInfo.Flow.Detail>();
        final MetaInfo.DeploymentGroup dg = ctx.getDeploymentGroup("default");
        final MetaInfo.Flow.Detail sourceAppDetail = new MetaInfo.Flow.Detail();
        MetaInfo.Flow monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, appName, null, authToken);
        sourceAppDetail.construct(deploymentStrategy, dg.uuid, monitoringApp.uuid, MetaInfo.Flow.Detail.FailOverRule.AUTO);
        deploymentPlan.add(sourceAppDetail);
        final List<MetaInfo.Flow> flows = monitoringApp.getSubFlows();
        for (final MetaInfo.Flow flow : flows) {
            if (flow.getName().equals("MonitoringSourceFlow")) {
                final MetaInfo.Flow.Detail sourceFlowDetail = new MetaInfo.Flow.Detail();
                sourceFlowDetail.construct(DeploymentStrategy.ON_ALL, dg.uuid, flow.uuid, MetaInfo.Flow.Detail.FailOverRule.AUTO);
                deploymentPlan.add(sourceFlowDetail);
            }
        }
        monitoringApp.setDeploymentPlan(deploymentPlan);
        monitoringApp.setFlowStatus(MetaInfo.StatusInfo.Status.RUNNING);
        ctx.put(monitoringApp);
        monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, appName, null, authToken);
        monitoringApp.getMetaInfoStatus().setAnonymous(true);
        instance.updateMetaObject(monitoringApp, authToken);
        if (monitoringApp == null) {
            MonitorApp.logger.warn((Object)"Failed to create Monitoring Application");
            return;
        }
        assert monitoringApp.deploymentPlan.size() > 0;
        assert monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING);
    }
    
    static {
        MonitorApp.logger = Logger.getLogger((Class)MonitorApp.class);
        MonitorApp.appStmt = "IMPORT com.datasphere.runtime.monitor.MonitorModel;\nIMPORT com.datasphere.runtime.monitor.MonitorBatchEvent;\n\nCREATE APPLICATION MonitoringSourceApp;\n\nCREATE OR REPLACE STREAM MonitoringSourceStream OF com.datasphere.runtime.monitor.MonitorBatchEvent;\n\nCREATE OR REPLACE STREAM MonitoringStream1 OF com.datasphere.runtime.monitor.MonitorBatchEvent;\n\nCREATE FLOW MonitoringSourceFlow;\n\nCREATE OR REPLACE SOURCE MonitoringSource1 USING MonitoringDataSource (\n )\nOUTPUT TO MonitoringSourceStream;\n\nEND FLOW MonitoringSourceFlow;\n\nEND APPLICATION MonitoringSourceApp;\n\nCREATE APPLICATION MonitoringProcessApp;\n\nCREATE OR REPLACE CQ MonitoringCQ\nINSERT INTO MonitoringStream1\nselect processBatch(m) from MonitoringSourceStream m;\n\nEND APPLICATION MonitoringProcessApp;";
    }
}
