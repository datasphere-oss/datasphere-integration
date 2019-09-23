package com.datasphere.rest.apiv2.tungsten;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.datasphere.exception.CompilationException;
import com.datasphere.exception.SecurityException;
import com.datasphere.rest.apiv2.Utility;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.Server;
import com.datasphere.uuid.AuthToken;

@Path("/tungsten")
@Consumes({ "text/plain" })
@Produces({ "text/plain" })
public class TungstenApi
{
    @POST
    @Consumes({ "text/plain" })
    @Produces({ "text/plain" })
    public Response compileTungstenStatement(final String tungstenCommandList, @Context final HttpServletRequest request) {
        AuthToken token = null;
        if (tungstenCommandList == null || tungstenCommandList.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)"Required parameter tungstenCommandList not passed").build();
        }
        final QueryValidator qv = new QueryValidator(Server.getServer());
        try {
            token = Utility.getAuthTokenFromAuthorizationHeader(request);
            qv.createNewContext(token);
            qv.setUpdateMode(true);
            final String warning = qv.compileText(token, tungstenCommandList);
            if (warning != null && !warning.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity((Object)warning).build();
            }
            return Response.ok().entity((Object)"Success").build();
        }
        catch (SecurityException se) {
            return Response.status(Response.Status.FORBIDDEN).entity((Object)"Forbidden").build();
        }
        catch (CompilationException ce) {
            return Response.status(Response.Status.BAD_REQUEST).entity((Object)ce.getMessage()).build();
        }
        catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity((Object)("Failed to compile statement : " + e.getMessage())).build();
        }
        finally {
            if (token != null) {
                qv.removeContext(token);
            }
        }
    }
}
