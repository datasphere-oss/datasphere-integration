package com.datasphere.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.RedirectException;
import org.apache.log4j.Logger;

import com.datasphere.jmqmessaging.StreamInfoResponse;
import com.datasphere.jmqmessaging.ZMQSystem;
import com.datasphere.messaging.MessagingProvider;
import com.datasphere.messaging.MessagingSystem;
import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.MetadataRepositoryUtils;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.uuid.UUID;

public class StreamInfoServlet extends HttpServlet
{
    private static final long serialVersionUID = 2538840396215530681L;
    private static final Logger logger;
    
    private List<DistLink> getSubscriberForStream(final String streamName) throws MetaDataRepositoryException, RedirectException {
        if (Server.server == null) {
            throw new IllegalStateException("getSubscriberForStream can only be called from a server node");
        }
        final String[] tokenized = streamName.split(":");
        if (tokenized.length != 2) {
            throw new IllegalArgumentException("Illegal stream name argument :" + streamName + ". Argument should be of format NAMESPACE:STREAMNAME");
        }
        final MetaInfo.Stream streamMetaObj = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, tokenized[0], tokenized[1], null, HSecurityManager.TOKEN);
        if (streamMetaObj == null) {
            throw new NoSuchElementException("Stream '" + streamName + "' does not exist");
        }
        final Stream streamObj = Server.server.getStreamRuntime(streamMetaObj.getUuid());
        if (streamObj == null) {
            final Set<MetaInfo.Server> servers = (Set<MetaInfo.Server>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, HSecurityManager.TOKEN);
            for (final MetaInfo.Server srv : servers) {
                if (!srv.isAgent) {
                    final Map<String, Set<UUID>> srvObjectUUIDs = srv.getCurrentUUIDs();
                    for (final String app : srvObjectUUIDs.keySet()) {
                        final Set<UUID> objects = srvObjectUUIDs.get(app);
                        if (objects.contains(streamMetaObj.uuid)) {
                            final String redirectUri = srv.getWebBaseUri() + "/streams/" + streamName;
                            if (StreamInfoServlet.logger.isTraceEnabled()) {
                                StreamInfoServlet.logger.trace((Object)("Stream not found on this node.Setting redirect URI to " + redirectUri));
                            }
                            throw new RedirectException(redirectUri);
                        }
                    }
                }
            }
        }
        if (streamObj == null) {
            throw new NoSuchElementException("Stream '" + streamName + "' does not exist");
        }
        final Set<DistLink> subList = streamObj.getDistributedChannel().getSubscribers();
        final List<DistLink> retVal = new ArrayList<DistLink>();
        retVal.addAll(subList);
        return retVal;
    }
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        String streamName = request.getPathInfo();
        if (streamName.startsWith("/")) {
            streamName = streamName.substring(1);
        }
        final int index = streamName.indexOf(58);
        if (index == -1) {
            StreamInfoServlet.logger.warn((Object)("Failure in retrieving stream information for " + streamName + ".Missing ':' from streamName"));
            response.setStatus(400);
            response.getWriter().write("Invalid streamName '" + streamName + "'. Missing colon character between namespace and name");
            return;
        }
        final String namespace = streamName.substring(0, index);
        final String name = streamName.substring(index + 1, streamName.length());
        if (Server.server == null) {
            response.setStatus(501);
            response.getWriter().write("Request received by an invalid HD Server");
            return;
        }
        final MessagingSystem msgSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
        if (!(msgSystem instanceof ZMQSystem)) {
            response.setStatus(501);
            response.getWriter().write("Request received by an invalid HD Server");
            return;
        }
        final ZMQSystem zmqMsgSystem = (ZMQSystem)msgSystem;
        Map.Entry<UUID, ReceiverInfo> streamDetails = null;
        List<DistLink> subscriberList = null;
        boolean isEncrypted = false;
        try {
            streamDetails = zmqMsgSystem.searchStreamName(streamName);
            if (streamDetails == null) {
                response.setStatus(404);
                response.getWriter().write("No Stream found for '" + streamName + "'");
                return;
            }
            final MetaInfo.Stream streamInfo = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, namespace, name, null, HSecurityManager.TOKEN);
            if (streamInfo == null) {
                response.setStatus(404);
                response.getWriter().write("No Stream found for '" + streamName + "'");
                return;
            }
            final MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(streamInfo);
            if (appInfo != null) {
                isEncrypted = appInfo.encrypted;
            }
        }
        catch (Exception e) {
            StreamInfoServlet.logger.warn((Object)("Failed to retrieve stream information for " + streamName), (Throwable)e);
            response.setStatus(404);
            response.getWriter().write("No Stream found for '" + streamName + "'");
            return;
        }
        try {
            subscriberList = this.getSubscriberForStream(streamName);
        }
        catch (NoSuchElementException nse2) {
            response.setStatus(404);
            response.getWriter().write("No Stream found for '" + streamName + "'");
            return;
        }
        catch (RedirectException nse) {
            response.setStatus(302);
            response.addHeader("Location", nse.getMessage());
            return;
        }
        catch (Exception e) {
            StreamInfoServlet.logger.warn((Object)("Failed to retrieve stream information for " + streamName), (Throwable)e);
            response.setStatus(404);
            response.getWriter().write("No Stream found for '" + streamName + "'");
            return;
        }
        final StreamInfoResponse streamResponse = new StreamInfoResponse((UUID)streamDetails.getKey(), (ReceiverInfo)streamDetails.getValue(), (List)subscriberList, isEncrypted);
        final byte[] data = KryoSingleton.write(streamResponse, false);
        response.setStatus(200);
        response.setContentType("application/x-hd-serialized");
        response.setContentLength(data.length);
        response.getOutputStream().write(data);
        response.getOutputStream().flush();
    }
    
    static {
        logger = Logger.getLogger((Class)StreamInfoServlet.class);
    }
}
