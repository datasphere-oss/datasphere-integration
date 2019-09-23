package com.datasphere.rest.apiv2.appmgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.datasphere.exception.CompilationException;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.rest.apiv2.ResponseMessage;
import com.datasphere.rest.apiv2.Utility;
import com.datasphere.rest.apiv2.appmgmt.models.AppDeploymentPlan;
import com.datasphere.rest.apiv2.appmgmt.models.Application;
import com.datasphere.rest.apiv2.appmgmt.models.ApplicationList;
import com.datasphere.rest.apiv2.appmgmt.models.ApplicationParameters;
import com.datasphere.rest.apiv2.appmgmt.models.FlowDeploymentPlan;
import com.datasphere.rest.apiv2.appmgmt.templates.ApplicationTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.DatabaseReaderToDatabaseWriterTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.DatabaseReaderToKafkaAvroTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.DatabaseReaderToKafkaJsonTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.OracleReaderToDatabaseWriterTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.OracleReaderToKafkaAvroTemplate;
import com.datasphere.rest.apiv2.appmgmt.templates.OracleReaderToKafkaJsonTemplate;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;

@Path("/applications")
@Consumes({ "application/json" })
@Produces({ "application/json", "text/csv" })
public class ApplicationsApi
{
    private static final Logger logger;
    private static final Map<String, ApplicationTemplate> templateIdToImplMap;
    
    public static Application convertMetaInfoToApplication(final MetaInfo.Flow application, final AuthToken token) throws NoSuchElementException, MetaDataRepositoryException {
        if (application == null) {
            throw new NoSuchElementException("No application found");
        }
        final String baseUri = "/api/v2/applications/";
        final Application app = new Application();
        app.setNamespace(application.nsName);
        app.setName(application.name);
        final MetaInfo.StatusInfo statusInfo = MetadataRepository.getINSTANCE().getStatusInfo(application.getUuid(), token);
        app.setStatus(statusInfo.getStatus().toString());
        app.addLink("self", new ArrayList<String>() {
            {
                this.add("GET");
                this.add("DELETE");
            }
        }, baseUri + application.getFullName());
        app.addLink("deployment", new ArrayList<String>() {
            {
                this.add("DELETE");
                this.add("POST");
            }
        }, baseUri + application.getFullName() + "/deployment");
        app.addLink("sprint", new ArrayList<String>() {
            {
                this.add("DELETE");
                this.add("POST");
            }
        }, baseUri + application.getFullName() + "/sprint");
        return app;
    }
    
    public static String[] validateAndParseAppFqn(final String appFqn) throws IllegalArgumentException {
        if (appFqn == null || appFqn.isEmpty()) {
            throw new IllegalArgumentException();
        }
        final String[] nsAndApp = appFqn.split("\\.");
        if (nsAndApp == null || nsAndApp.length != 2) {
            throw new IllegalArgumentException();
        }
        return nsAndApp;
    }
    
    public static List<String> retrieveFlowNames(final MetaInfo.Flow application) throws MetaDataRepositoryException {
        final List<MetaInfo.Flow> subFlows = application.getSubFlows();
        final List<String> returnList = new ArrayList<String>();
        for (final MetaInfo.Flow flow : subFlows) {
            returnList.add(flow.getName());
        }
        return returnList;
    }
    
    @POST
    @Path("/{appFqn}/deployment")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response deployAppById(@PathParam("appFqn") final String appFqn, final AppDeploymentPlan deploymentPlan, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeDeploy = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeDeploy == null) {
                throw new NoSuchElementException();
            }
            final Map<String, String> appDescription = new HashMap<String, String>();
            appDescription.put("flow", appFqn);
            appDescription.put("strategy", "any");
            final List<String> subFlows = retrieveFlowNames(applicationBeforeDeploy);
            final int applicationSubFlowCount = subFlows.size();
            final int requestSubFlowCount = deploymentPlan.size();
            final List<String> deploymentGroups = Server.getServer().getDeploymentGroups();
            if (applicationSubFlowCount != requestSubFlowCount) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Count of flows in DeploymentPlan do not match the application flow: Application contains " + applicationBeforeDeploy.getSubFlows().size() + " flows and requested deployment plan contains " + deploymentPlan.size())).build();
            }
            Map<String, String>[] detailDescriptions = null;
            if (deploymentPlan != null && deploymentPlan.size() > 0) {
                detailDescriptions = (Map<String, String>[])new Map[deploymentPlan.size()];
                int i = 0;
                for (final FlowDeploymentPlan fdp : deploymentPlan) {
                    detailDescriptions[i] = new HashMap<String, String>();
                    final String requestedFlowName = fdp.getFlowName();
                    if (!subFlows.contains(requestedFlowName)) {
                        return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No subflow found by the name '" + requestedFlowName + "' in application")).build();
                    }
                    detailDescriptions[i].put("flow", requestedFlowName);
                    detailDescriptions[i].put("strategy", fdp.getDeploymentType().value().toLowerCase());
                    final String requestedDeploymentGroup = fdp.getDeploymentGroupName();
                    if (!deploymentGroups.contains(requestedDeploymentGroup)) {
                        return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No deployment group found for '" + requestedDeploymentGroup + "'")).build();
                    }
                    detailDescriptions[i].put("group", requestedDeploymentGroup);
                    ++i;
                }
            }
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(token);
            queryValidator.setUpdateMode(true);
            queryValidator.setCurrentApp(appFqn, token);
            queryValidator.CreateDeployFlowStatement(token, EntityType.APPLICATION.name(), appDescription, detailDescriptions);
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            final Application app = convertMetaInfoToApplication(application, token);
            return Response.ok().entity((Object)app).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message(mdre.getMessage())).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @GET
    @Path("/{appFqn}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getApplicationByName(@PathParam("appFqn") final String appFqn, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            final Application app = convertMetaInfoToApplication(application, token);
            return Response.ok().entity((Object)app).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
    }
    
    @GET
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getListOfAllApps(@Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final List<MetaInfo.Flow> applications = MetadataRepository.getINSTANCE().getAllApplications(token);
            final ApplicationList list = new ApplicationList();
            for (final MetaInfo.Flow application : applications) {
                final Application app = convertMetaInfoToApplication(application, token);
                list.add(app);
            }
            return Response.ok().entity((Object)list).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    @POST
    @Path("/{appFqn}/sprint")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response startAppById(@PathParam("appFqn") final String appFqn, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeDeploy = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeDeploy == null) {
                throw new NoSuchElementException();
            }
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(token);
            queryValidator.setUpdateMode(true);
            queryValidator.setCurrentApp(appFqn, token);
            queryValidator.CreateStartFlowStatement(token, appFqn, EntityType.APPLICATION.name());
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            final Application app = convertMetaInfoToApplication(application, token);
            return Response.ok().entity((Object)app).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message("Unable to start app: " + mdre.getMessage())).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @DELETE
    @Path("/{appFqn}/sprint")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response stopAppById(@PathParam("appFqn") final String appFqn, @DefaultValue("false") @QueryParam("quiesce") final Boolean quiesce, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeDeploy = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeDeploy == null) {
                throw new NoSuchElementException();
            }
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(token);
            queryValidator.setUpdateMode(true);
            queryValidator.setCurrentApp(appFqn, token);
            if (quiesce) {
                queryValidator.CreateQuiesceFlowStatement(token, appFqn, EntityType.APPLICATION.name());
            }
            else {
                queryValidator.CreateStopFlowStatement(token, appFqn, EntityType.APPLICATION.name());
            }
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            final Application app = convertMetaInfoToApplication(application, token);
            return Response.ok().entity((Object)app).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message("Unable to start app: " + mdre.getMessage())).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @DELETE
    @Path("/{appFqn}/deployment")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response undeployAppById(@PathParam("appFqn") final String appFqn, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeDeploy = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeDeploy == null) {
                throw new NoSuchElementException();
            }
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(token);
            queryValidator.setUpdateMode(true);
            queryValidator.setCurrentApp(appFqn, token);
            queryValidator.CreateUndeployFlowStatement(token, appFqn, EntityType.APPLICATION.name());
            final MetaInfo.Flow application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            final Application app = convertMetaInfoToApplication(application, token);
            return Response.ok().entity((Object)app).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message("Unable to undeploy app: " + mdre.getMessage())).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @POST
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response createApplication(final ApplicationParameters applicationParameters, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String appFqn = applicationParameters.getApplicationName();
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeCreate = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeCreate != null) {
                return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message("Application of the same name already exists :" + appFqn)).build();
            }
            final String templateId = applicationParameters.getTemplateId();
            if (!ApplicationsApi.templateIdToImplMap.containsKey(templateId)) {
                return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No Template found for the name '" + templateId + "'")).build();
            }
            final ApplicationTemplate applicationTemplate = ApplicationsApi.templateIdToImplMap.get(templateId);
            applicationTemplate.createApplication(token, applicationParameters);
            final MetaInfo.Flow applicationAfterCreate = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationAfterCreate == null) {
                return Response.serverError().entity((Object)new ResponseMessage().message("Failed to create application")).build();
            }
            return Response.ok().entity((Object)convertMetaInfoToApplication(applicationAfterCreate, token)).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message("Unable to create app: " + mdre.getMessage())).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @DELETE
    @Path("/{appFqn}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response dropsAppById(@PathParam("appFqn") final String appFqn, @Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String[] nsAndApp = validateAndParseAppFqn(appFqn);
            final MetaInfo.Flow applicationBeforeDrop = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, nsAndApp[0], nsAndApp[1], null, token);
            if (applicationBeforeDrop == null) {
                throw new NoSuchElementException();
            }
            final QueryValidator queryValidator = new QueryValidator(Server.getServer());
            queryValidator.createNewContext(token);
            queryValidator.setUpdateMode(true);
            queryValidator.setCurrentApp(appFqn, token);
            queryValidator.DropStatement(token, EntityType.APPLICATION.toString(), appFqn, true, true);
            return Response.ok((Object)new ResponseMessage().message("Application dropped successfully:  " + appFqn)).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.serverError().entity((Object)new ResponseMessage().message("Unable to undeploy app: " + mdre.getMessage())).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No application found for the specified parameter '" + appFqn + "'")).build();
        }
        catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Illegal argument passed for appFqn. The argument should be non-empty and of format 'namespace.appname'")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message(ce.getMessage())).build();
        }
        catch (Warning warning) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity((Object)new ResponseMessage().message(warning.getMessage())).build();
        }
        catch (RuntimeException re) {
            return Response.serverError().entity((Object)new ResponseMessage().message(re.getMessage())).build();
        }
    }
    
    @GET
    @Path("/templates")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getListOfAllTemplates(@Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String username = HSecurityManager.getAutheticatedUserName(token);
            if (username == null) {
                throw new SecurityException();
            }
            final List<String> stencilList = new ArrayList<String>();
            for (final String stencilName : ApplicationsApi.templateIdToImplMap.keySet()) {
                stencilList.add(stencilName);
            }
            return Response.ok().entity((Object)stencilList).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
    }
    
    @GET
    @Path("/templates/{templateId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getTemplateById(@PathParam("templateId") final String templateId, @Context final HttpServletRequest request) {
        if (templateId == null || templateId.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter templateId not passed")).build();
        }
        if (!ApplicationsApi.templateIdToImplMap.containsKey(templateId)) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No Template found for the name '" + templateId + "'")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            return Response.ok().entity((Object)ApplicationsApi.templateIdToImplMap.get(templateId).getApplicationTemplateModel(token)).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity((Object)new ResponseMessage().message(mdre.getMessage())).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
    }
    
    static {
        logger = Logger.getLogger((Class)ApplicationsApi.class);
        templateIdToImplMap = new HashMap<String, ApplicationTemplate>();
        final DatabaseReaderToDatabaseWriterTemplate template1 = new DatabaseReaderToDatabaseWriterTemplate();
        final OracleReaderToDatabaseWriterTemplate template2 = new OracleReaderToDatabaseWriterTemplate();
        final DatabaseReaderToKafkaAvroTemplate template3 = new DatabaseReaderToKafkaAvroTemplate();
        final DatabaseReaderToKafkaJsonTemplate template4 = new DatabaseReaderToKafkaJsonTemplate();
        final OracleReaderToKafkaAvroTemplate template5 = new OracleReaderToKafkaAvroTemplate();
        final OracleReaderToKafkaJsonTemplate template6 = new OracleReaderToKafkaJsonTemplate();
        ApplicationsApi.templateIdToImplMap.put(template1.getTemplateId(), template1);
        ApplicationsApi.templateIdToImplMap.put(template2.getTemplateId(), template2);
        ApplicationsApi.templateIdToImplMap.put(template3.getTemplateId(), template3);
        ApplicationsApi.templateIdToImplMap.put(template4.getTemplateId(), template4);
        ApplicationsApi.templateIdToImplMap.put(template5.getTemplateId(), template5);
        ApplicationsApi.templateIdToImplMap.put(template6.getTemplateId(), template6);
    }
}
