package com.datasphere.rest.apiv2.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.datasphere.exception.SecurityException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.rest.apiv2.ResponseMessage;
import com.datasphere.rest.apiv2.Utility;
import com.datasphere.rest.apiv2.appmgmt.ApplicationsApi;
import com.datasphere.rest.apiv2.appmgmt.models.ApplicationList;
import com.datasphere.rest.apiv2.integration.models.Topology;
import com.datasphere.rest.apiv2.integration.models.TopologyList;
import com.datasphere.rest.apiv2.integration.models.TopologyRequest;
import com.datasphere.rest.apiv2.integration.stencils.IStencil;
import com.datasphere.rest.apiv2.integration.stencils.OracleToSqldbStencil;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;

@Path("/integration")
@Consumes({ "application/json" })
@Produces({ "application/json" })
public class IntegrationApi
{
    private static Map<String, IStencil> stencilIdtoImplMap;
    
    public static Topology converNamespaceIntoTopology(final String namespace, final AuthToken token, final boolean addApplications) throws MetaDataRepositoryException, NoSuchElementException, SecurityException {
        final Topology topology = new Topology();
        if (addApplications) {
            final Set<MetaInfo.Flow> topologyObjects = (Set<MetaInfo.Flow>)MetadataRepository.getINSTANCE().getByEntityTypeInNameSpace(EntityType.APPLICATION, namespace, token);
            if (topologyObjects == null || topologyObjects.isEmpty()) {
                throw new NoSuchElementException("No application exists in namespace '" + namespace + "'");
            }
            final ApplicationList list = new ApplicationList();
            for (final MetaInfo.MetaObject topologyObject : topologyObjects) {
                list.add(ApplicationsApi.convertMetaInfoToApplication((MetaInfo.Flow)topologyObject, token));
            }
            topology.applicationList(list);
        }
        else {
            topology.applicationList(null);
        }
        topology.topologyName(namespace);
        topology.addLink("self", new ArrayList<String>() {
            {
                this.add("GET");
                this.add("DELETE");
            }
        }, "/api/v2/integration/topologies/" + namespace);
        topology.addLink("self", new ArrayList<String>() {
            {
                this.add("POST");
                this.add("DELETE");
            }
        }, "/api/v2/integration/topologies/" + namespace + "/deployment");
        return topology;
    }
    
    @GET
    @Path("/stencils")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getListOfAllStencils(@Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String username = HSecurityManager.getAutheticatedUserName(token);
            if (username == null) {
                throw new SecurityException();
            }
            final List<String> stencilList = new ArrayList<String>();
            for (final String stencilName : IntegrationApi.stencilIdtoImplMap.keySet()) {
                stencilList.add(stencilName);
            }
            return Response.ok().entity((Object)stencilList).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
    }
    
    @GET
    @Path("/stencils/{stencilId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getStencilById(@PathParam("stencilId") final String stencilId, @Context final HttpServletRequest request) {
        if (stencilId == null || stencilId.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter StencilId not passed")).build();
        }
        if (!IntegrationApi.stencilIdtoImplMap.containsKey(stencilId)) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No Stencil found for the name '" + stencilId + "'")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            return Response.ok().entity((Object)IntegrationApi.stencilIdtoImplMap.get(stencilId).getStencilModel(token)).build();
        }
        catch (MetaDataRepositoryException mdre) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity((Object)new ResponseMessage().message(mdre.getMessage())).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
    }
    
    @POST
    @Path("/topologies")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response createTopology(final TopologyRequest topologyRequestParameters, @Context final HttpServletRequest request) {
        if (topologyRequestParameters == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter TopologyRequest not found")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final String stencilName = topologyRequestParameters.getStencilId();
            if (stencilName == null || stencilName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter stencilName not found in TopologyParameter")).build();
            }
            final IStencil stencil = IntegrationApi.stencilIdtoImplMap.get(stencilName);
            if (stencilName == null || stencilName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("No Stencil found for the name " + stencilName)).build();
            }
            final String namespace = topologyRequestParameters.getTopologyName();
            final MetaInfo.MetaObject propset = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, namespace, "StencilProperties", null, token);
            if (propset != null && !propset.getMetaInfoStatus().isDropped()) {
                return Response.status(Response.Status.CONFLICT).entity((Object)new ResponseMessage().message("Topology cannot be created as topology with the same name exists: '" + namespace + "'")).build();
            }
            stencil.createTopology(token, topologyRequestParameters);
            final Topology topology = converNamespaceIntoTopology(namespace, token, true);
            return Response.ok((Object)topology).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity((Object)new ResponseMessage().message("Could not create topology")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    @GET
    @Path("/topologies")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getListOfAllTopologies(@Context final HttpServletRequest request) {
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final Set<MetaInfo.PropertySet> propsetSet = (Set<MetaInfo.PropertySet>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.PROPERTYSET, token);
            final TopologyList list = new TopologyList();
            for (final MetaInfo.PropertySet propset : propsetSet) {
                if ("StencilProperties".equals(propset.getName())) {
                    final Topology topology = converNamespaceIntoTopology(propset.getNsName(), token, false);
                    list.add(topology);
                }
            }
            return Response.ok((Object)list).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No topology found")).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    @DELETE
    @Path("/topologies/{topologyName}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response deleteTopologyByName(@PathParam("topologyName") final String topologyName, @Context final HttpServletRequest request) {
        if (topologyName == null || topologyName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter TopologyRequest not found")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final MetaInfo.PropertySet propset = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, topologyName, "StencilProperties", null, token);
            if (propset == null || propset.getMetaInfoStatus().isDropped()) {
                throw new NoSuchElementException();
            }
            final String stencilName = (String)propset.getProperties().get("StencilName");
            if (stencilName == null || stencilName.isEmpty()) {
                throw new NoSuchElementException();
            }
            final IStencil stencil = IntegrationApi.stencilIdtoImplMap.get(stencilName);
            if (stencilName == null || stencilName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("No Stencil found for the name " + stencilName)).build();
            }
            stencil.dropTopology(token, topologyName);
            return Response.ok((Object)new ResponseMessage().message("Deleted topology successfully:  " + topologyName)).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (NoSuchElementException nse) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No topology found for '" + topologyName + "'")).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    @GET
    @Path("/topologies/{topologyName}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response getTopologyByName(@PathParam("topologyName") final String topologyName, @Context final HttpServletRequest request) {
        if (topologyName == null || topologyName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter TopologyRequest not found")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final Topology topology = converNamespaceIntoTopology(topologyName, token, true);
            return Response.ok((Object)topology).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No topology found for '" + topologyName + "'")).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)e.getMessage()).build();
        }
    }
    
    @POST
    @Path("/topologies/{topologyName}/deployment")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response startTopologyByName(@PathParam("topologyName") final String topologyName, @Context final HttpServletRequest request) {
        if (topologyName == null || topologyName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter TopologyRequest not found")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final MetaInfo.PropertySet propset = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, topologyName, "StencilProperties", null, token);
            if (propset == null || propset.getMetaInfoStatus().isDropped()) {
                throw new NoSuchElementException();
            }
            final String stencilName = (String)propset.getProperties().get("StencilName");
            if (stencilName == null || stencilName.isEmpty()) {
                throw new NoSuchElementException();
            }
            final IStencil stencil = IntegrationApi.stencilIdtoImplMap.get(stencilName);
            if (stencilName == null || stencilName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("No Stencil found for the name " + stencilName)).build();
            }
            stencil.startTopology(token, topologyName);
            final Topology topology = converNamespaceIntoTopology(topologyName, token, true);
            return Response.ok((Object)topology).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)new ResponseMessage().message("No topology found for '" + topologyName + "'")).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    @DELETE
    @Path("/topologies/{topologyName}/deployment")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    public Response stopTopologyByName(@PathParam("topologyName") final String topologyName, @Context final HttpServletRequest request) {
        if (topologyName == null || topologyName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("Required parameter TopologyRequest not found")).build();
        }
        try {
            final AuthToken token = Utility.getAuthTokenFromAuthorizationHeader(request);
            final MetaInfo.PropertySet propset = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, topologyName, "StencilProperties", null, token);
            if (propset == null || propset.getMetaInfoStatus().isDropped()) {
                throw new NoSuchElementException();
            }
            final String stencilName = (String)propset.getProperties().get("StencilName");
            if (stencilName == null || stencilName.isEmpty()) {
                throw new NoSuchElementException();
            }
            final IStencil stencil = IntegrationApi.stencilIdtoImplMap.get(stencilName);
            if (stencilName == null || stencilName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)new ResponseMessage().message("No Stencil found for the name " + stencilName)).build();
            }
            stencil.stopTopology(token, topologyName);
            final Topology topology = converNamespaceIntoTopology(topologyName, token, true);
            return Response.ok((Object)topology).build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        catch (NoSuchElementException nsee) {
            return Response.status(Response.Status.NOT_FOUND).entity((Object)("No topology found for '" + topologyName + "'")).build();
        }
        catch (Exception e) {
            return Response.serverError().entity((Object)new ResponseMessage().message(e.getMessage())).build();
        }
    }
    
    static {
        IntegrationApi.stencilIdtoImplMap = new HashMap<String, IStencil>();
        final OracleToSqldbStencil stencil = new OracleToSqldbStencil();
        IntegrationApi.stencilIdtoImplMap.put("oracle-to-sqldb", stencil);
    }
}
